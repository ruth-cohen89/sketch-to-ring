import sharp from "sharp";
import crypto from "node:crypto";

export class BadImage extends Error {}

const MAX_INPUT_PIXELS = 4096 * 4096; // decoded-size cap (decompression bomb guard)

/** Validates PNG/JPEG, normalizes to <=1024x1024 on white background (aspect kept). */
export async function normalizeUpload(buf: Buffer) {
  let meta;
  try {
    meta = await sharp(buf, { limitInputPixels: MAX_INPUT_PIXELS }).metadata();
  } catch {
    throw new BadImage("Image could not be decoded");
  }
  if (meta.format !== "png" && meta.format !== "jpeg") throw new BadImage("Only PNG or JPEG allowed");
  try {
    const { data, info } = await sharp(buf, { limitInputPixels: MAX_INPUT_PIXELS })
      .rotate()
      .resize(1024, 1024, { fit: "inside", withoutEnlargement: true })
      .flatten({ background: "#ffffff" })
      .png()
      .toBuffer({ resolveWithObject: true });
    return { png: data, width: info.width, height: info.height, hash: sha256(data) };
  } catch {
    throw new BadImage("Image could not be processed");
  }
}

export const sha256 = (b: Buffer) => crypto.createHash("sha256").update(b).digest("hex");

export async function dims(buf: Buffer) {
  const m = await sharp(buf).metadata();
  return { width: m.width ?? 0, height: m.height ?? 0 };
}
