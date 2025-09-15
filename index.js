import Fastify from 'fastify';
import * as Minio from 'minio';
import fastifyMultipart from '@fastify/multipart';
import fastifyCors from '@fastify/cors';

// Configure MinIO client
const minioClient = new Minio.Client({
  endPoint: 'localhost',
  port: 9000,
  useSSL: false, // Set to true if using HTTPS
  accessKey: 'minioadmin', // Default MinIO credentials
  secretKey: 'minioadmin'  // Change these in production
});

const BUCKET_NAME = 'my-bucket'; // Change to your bucket name

// Ensure bucket exists
async function ensureBucket() {
  try {
    const exists = await minioClient.bucketExists(BUCKET_NAME);
    if (!exists) {
      await minioClient.makeBucket(BUCKET_NAME, 'us-east-1');
      console.log(`Bucket '${BUCKET_NAME}' created successfully`);
    } else {
      console.log(`Bucket '${BUCKET_NAME}' already exists`);
    }
  } catch (err) {
    console.error('Error with bucket:', err);
  }
}

const fastify = Fastify({ logger: true });

fastify.register(fastifyCors, {
  origin: "*", // Allow all origins (dev mode)
  methods: ["GET", "POST", "PUT", "DELETE"],
});

// Register multipart plugin for file uploads
fastify.register(fastifyMultipart, {
  limits: {
    fieldNameSize: 100,
    fieldSize: 100,
    fields: 10,
    fileSize: 10 * 1024 * 1024, // 10MB
    files: 5
  }
});

// Health check route
fastify.get('/health', async (request, reply) => {
  return { status: 'ok', message: 'MinIO Fastify server is running' };
});

// Upload file route
fastify.post('/upload', async (request, reply) => {
  try {
    const data = await request.file();

    if (!data) {
      return reply.code(400).send({ error: 'No file uploaded' });
    }

    const filename = data.filename;
    const mimetype = data.mimetype;

    // Generate unique filename to avoid conflicts
    const uniqueFilename = `${Date.now()}-${filename}`;

    // Upload to MinIO
    await minioClient.putObject(BUCKET_NAME, uniqueFilename, data.file);

    return {
      success: true,
      message: 'File uploaded successfully',
      filename: uniqueFilename,
      originalName: filename,
      mimetype: mimetype
    };

  } catch (error) {
    console.error('Upload error:', error);
    return reply.code(500).send({ error: 'Failed to upload file' });
  }
});

// Get/Download file route
fastify.get('/file/:filename', async (request, reply) => {
  try {
    const { filename } = request.params;

    // Get object from MinIO
    const dataStream = await minioClient.getObject(BUCKET_NAME, filename);

    // Get object metadata
    const stat = await minioClient.statObject(BUCKET_NAME, filename);

    // Set appropriate headers
    reply.type(stat.metaData['content-type'] || 'application/octet-stream');
    reply.header('Content-Length', stat.size);

    // If original name exists in metadata, use it for download
    if (stat.metaData['x-amz-meta-original-name']) {
      reply.header('Content-Disposition', `attachment; filename="${stat.metaData['x-amz-meta-original-name']}"`);
    }

    return reply.send(dataStream);

  } catch (error) {
    console.error('Download error:', error);
    if (error.code === 'NoSuchKey') {
      return reply.code(404).send({ error: 'File not found' });
    }
    return reply.code(500).send({ error: 'Failed to retrieve file' });
  }
});

// List all files in bucket
fastify.get('/files', async (request, reply) => {
  try {
    const objectsList = [];
    const stream = minioClient.listObjects(BUCKET_NAME, '', true);

    for await (const obj of stream) {
      // Get additional metadata for each object
      try {
        const stat = await minioClient.statObject(BUCKET_NAME, obj.name);
        objectsList.push({
          name: obj.name,
          originalName: stat.metaData['x-amz-meta-original-name'] || obj.name,
          size: obj.size,
          lastModified: obj.lastModified,
          contentType: stat.metaData['content-type']
        });
      } catch (err) {
        // If stat fails, just add basic info
        objectsList.push({
          name: obj.name,
          size: obj.size,
          lastModified: obj.lastModified
        });
      }
    }

    return {
      bucket: BUCKET_NAME,
      files: objectsList,
      count: objectsList.length
    };

  } catch (error) {
    console.error('List files error:', error);
    return reply.code(500).send({ error: 'Failed to list files' });
  }
});

// Delete file route
fastify.delete('/file/:filename', async (request, reply) => {
  try {
    const { filename } = request.params;

    // Check if file exists first
    await minioClient.statObject(BUCKET_NAME, filename);

    // Delete the object
    await minioClient.removeObject(BUCKET_NAME, filename);

    return {
      success: true,
      message: `File '${filename}' deleted successfully`
    };

  } catch (error) {
    console.error('Delete error:', error);
    if (error.code === 'NoSuchKey') {
      return reply.code(404).send({ error: 'File not found' });
    }
    return reply.code(500).send({ error: 'Failed to delete file' });
  }
});

// Get presigned URL for direct upload (useful for frontend)
fastify.get('/upload-url/:filename', async (request, reply) => {
  try {
    const { filename } = request.params;
    const expiry = 24 * 60 * 60; // 24 hours

    const presignedUrl = await minioClient.presignedPutObject(BUCKET_NAME, filename, expiry);

    return {
      uploadUrl: presignedUrl,
      filename: filename,
      expiresIn: expiry
    };

  } catch (error) {
    console.error('Presigned URL error:', error);
    return reply.code(500).send({ error: 'Failed to generate upload URL' });
  }
});

// Start server
const start = async () => {
  try {
    // Ensure bucket exists before starting server
    await ensureBucket();

    await fastify.listen({ port: 3000, host: '0.0.0.0' });
    console.log('Server is running on http://localhost:3000');
    console.log('Available endpoints:');
    console.log('  POST /upload - Upload a file');
    console.log('  GET /file/:filename - Download a file');
    console.log('  GET /files - List all files');
    console.log('  DELETE /file/:filename - Delete a file');
    console.log('  GET /upload-url/:filename - Get presigned upload URL');
    console.log('  GET /health - Health check');
  } catch (err) {
    fastify.log.error(err);
    process.exit(1);
  }
};

start();