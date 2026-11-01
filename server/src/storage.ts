import {
  S3Client,
  PutObjectCommand,
  GetObjectCommand,
  CreateBucketCommand,
  HeadBucketCommand,
} from "@aws-sdk/client-s3";
import { config } from "./config.js";

// SeaweedFS S3 gateway (shared network storage). Bucket is private: no public ACL, served only via Node.
const s3 = new S3Client({
  endpoint: config.s3.endpoint,
  region: "us-east-1",
  forcePathStyle: true,
  credentials: { accessKeyId: config.s3.accessKey, secretAccessKey: config.s3.secretKey },
});

export async function ensureBucket() {
  for (let i = 1; ; i++) {
    try {
      try {
        await s3.send(new HeadBucketCommand({ Bucket: config.s3.bucket }));
      } catch (e: any) {
        if (e?.$metadata?.httpStatusCode !== 404) throw e;
        await s3.send(new CreateBucketCommand({ Bucket: config.s3.bucket }));
      }
      return;
    } catch (e) {
      if (i >= 15) throw e;
      console.warn(`[storage] SeaweedFS not ready (${(e as Error).message}), retry ${i}`);
      await new Promise((r) => setTimeout(r, 2000));
    }
  }
}

export async function putObject(key: string, body: Buffer, contentType = "image/png") {
  await s3.send(new PutObjectCommand({ Bucket: config.s3.bucket, Key: key, Body: body, ContentType: contentType }));
}

export async function getObject(key: string): Promise<Buffer> {
  const r = await s3.send(new GetObjectCommand({ Bucket: config.s3.bucket, Key: key }));
  return Buffer.from(await r.Body!.transformToByteArray());
}
