import { config } from "./config.js";

type Design = Record<string, string | undefined>;

// Templates as data so the debug view can show the exact template that was rendered.
// A line whose {field} is unknown is dropped (no invented defaults).
export const GENERATE_TEMPLATE = [
  "A photorealistic studio product photograph of a single finger ring, following the shape and structure of the reference sketch.",
  "Metal: {metal}.",
  "Main stone: {stone}.",
  "Stone shape: {shape}.",
  "Style: {style}.",
  "Band: {band}.",
  "Customer notes: {notes}.",
  "Three-quarter view, clean white background, soft studio lighting, realistic metal reflections, sharp focus, high detail. No text, no hands, no people.",
];
export const REFINE_TEMPLATE = [
  "Refine this ring photograph: keep exactly the same ring structure, shape, proportions and design.",
  "Metal: {metal}.",
  "Stone: {stone}.",
  "Improve sharpness, lighting, realism of metal and gemstone reflections. Clean white background, professional jewelry catalog photo. No text.",
];

function render(template: string[], values: Design) {
  const used: Record<string, string> = {};
  const omitted: string[] = [];
  const lines = template.filter((line) => {
    const m = line.match(/\{(\w+)\}/);
    if (!m) return true;
    if (values[m[1]]) { used[m[1]] = values[m[1]]!; return true; }
    omitted.push(m[1]);
    return false;
  });
  const prompt = lines.map((l) => l.replace(/\{(\w+)\}/g, (_, k) => values[k]!)).join(" ");
  return { prompt, used, omitted };
}

// Only FLUX.2 fields we verified: prompt, input_image_0, width, height. No negative_prompt/strength/guidance/seed.
export function buildGeneratePrompt(design: Design, description: string) {
  return render(GENERATE_TEMPLATE, { ...design, notes: description.replace(/\s+/g, " ").slice(0, 300) || undefined });
}

export function buildRefinePrompt(design: Design) {
  return render(REFINE_TEMPLATE, design);
}

export const genOptions = () => ({ width: config.genWidth, height: config.genHeight });
export const modelId = () => (config.provider === "cloudflare" ? config.cfModel : "fake-provider");
