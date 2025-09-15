import Fastify from 'fastify';
import * as Minio from 'minio';
import fastifyCors from '@fastify/cors';

const fastify = Fastify({ logger: true });

// Enable CORS for local testing
fastify.register(fastifyCors, { origin: "*" });

// MinIO config
const minioClient = new Minio.Client({
  endPoint: 'localhost',
  port: 9000,
  useSSL: false,
  accessKey: 'minioadmin',
  secretKey: 'minioadmin'
});

const BUCKET_NAME = "my-bucket";

// In-memory store for posts (replace with DB in production)
const posts = [];

// Ensure bucket exists
async function ensureBucket() {
  const exists = await minioClient.bucketExists(BUCKET_NAME);
  if (!exists) {
    await minioClient.makeBucket(BUCKET_NAME, 'us-east-1');
    console.log(`Bucket '${BUCKET_NAME}' created`);
  }
}

// Route: Generate pre-signed upload URL
fastify.get('/upload-url/:filename', async (req, reply) => {
  try {
    const { filename } = req.params;
    const expiry = 60 * 5; // 5 minutes

    const presignedUrl = await minioClient.presignedPutObject(BUCKET_NAME, filename, expiry);

    return {
      uploadUrl: presignedUrl,
      fileKey: filename, // Save in DB later
      expiresIn: expiry
    };
  } catch (err) {
    req.log.error(err);
    return reply.code(500).send({ error: "Failed to create presigned URL" });
  }
});

// Route: Submit JSON form (metadata + fileKey)
fastify.post('/posts', async (req, reply) => {
  const { title, description, fileKey } = req.body;

  const post = {
    id: posts.length + 1,
    title,
    description,
    fileKey
  };

  posts.push(post);

  return { success: true, message: "Post created", post };
});

// Route: List all posts
fastify.get('/posts', async () => {
  return posts;
});

// Route: Get presigned URL for downloading a file
fastify.get('/download-url/:fileKey', async (req, reply) => {
  try {
    const { fileKey } = req.params;
    const url = await minioClient.presignedGetObject(BUCKET_NAME, fileKey, 60 * 5);
    return { url };
  } catch (err) {
    req.log.error(err);
    return reply.code(500).send({ error: "Failed to get download URL" });
  }
});

const start = async () => {
  await ensureBucket();
  await fastify.listen({ port: 3000, host: '0.0.0.0' });
  console.log("Server running on http://localhost:3000");
};

start();
