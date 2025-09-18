#!/usr/bin/env python3
import argparse
import boto3
import json
import base64
from pathlib import Path
from PIL import Image
import io

def resize_image_if_needed(img, max_dimension=8000):
    """Resize image if any dimension exceeds max_dimension"""
    width, height = img.size

    if width > max_dimension or height > max_dimension:
        # Calculate the scaling factor
        scale_factor = min(max_dimension / width, max_dimension / height)
        new_width = int(width * scale_factor)
        new_height = int(height * scale_factor)

        print(f"    Resizing from {width}x{height} to {new_width}x{new_height}")
        return img.resize((new_width, new_height), Image.Resampling.LANCZOS)

    return img

def encode_image(image_path):
    with open(image_path, "rb") as img_file:
        return base64.b64encode(img_file.read()).decode('utf-8')

def analyze_and_select_images(folder_path, N):
    bedrock = boto3.client(
        service_name='bedrock-runtime',
        region_name='us-west-2'
    )

    product_images_path = Path(folder_path) / 'product_images'

    if not product_images_path.exists():
        raise ValueError(f"Product images folder not found: {product_images_path}")

    image_files = list(product_images_path.glob('*.jpg')) + \
                  list(product_images_path.glob('*.jpeg')) + \
                  list(product_images_path.glob('*.png'))

    if not image_files:
        raise ValueError(f"No images found in {product_images_path}")

    print(f"Found {len(image_files)} product images")

    messages = [
        {
            "role": "user",
            "content": []
        }
    ]

    content_parts = []
    content_parts.append({
        "type": "text",
        "text": f"""You are an expert in selecting the best product images for video advertisement creation.

Analyze all the provided product images and select the most suitable ones for creating compelling video ads.

CRITICAL REQUIREMENTS:
- PRIORITIZE images with NO TEXT, labels, logos, or written content
- Avoid images with product names, brand text, or any overlaid text
- Select clean, text-free product shots

Consider the following criteria in order of importance:
1. NO TEXT OR WRITING - This is the most important criteria
2. Clean product presentation without text overlays
3. Image quality and resolution
4. Product visibility and clarity (full product view preferred)
5. Visual appeal and composition
6. Variety of angles or features shown
7. Potential for creating dynamic video content

Please select the best {N} images that are TEXT-FREE and would work well for video ad generation.

For each selected image, provide:
- Filename
- Reason for selection
- Key visual features
- Text presence assessment (none/minimal/significant)
- Suggested use in video ad

Return your response in JSON format with the following structure:
{{
    "selected_images": [
        {{
            "filename": "image_name.jpg",
            "reason": "Why this image was selected - MUST mention text-free status",
            "visual_features": "Key visual features",
            "text_presence": "none/minimal/significant",
            "suggested_use": "How to use in video ad"
        }}
    ],
    "summary": "Overall assessment focusing on text-free quality of selected images"
}}"""
    })

    for idx, img_path in enumerate(image_files):
        print(f"Processing image {idx + 1}: {img_path.name}")

        img = Image.open(img_path)
        if img.mode == 'RGBA':
            img = img.convert('RGB')

        # Resize if needed to stay within max dimension limit
        img = resize_image_if_needed(img, max_dimension=7500)

        buffer = io.BytesIO()
        img.save(buffer, format='JPEG', quality=85)
        img_base64 = base64.b64encode(buffer.getvalue()).decode('utf-8')

        content_parts.append({
            "type": "image",
            "source": {
                "type": "base64",
                "media_type": "image/jpeg",
                "data": img_base64
            }
        })

        content_parts.append({
            "type": "text",
            "text": f"Image {idx + 1}: {img_path.name}"
        })

    messages[0]["content"] = content_parts

    request_body = {
        "anthropic_version": "bedrock-2023-05-31",
        "messages": messages,
        "system": "You are an expert in visual content analysis for advertising.",
        "max_tokens": 4096,
        "temperature": 0.3
    }

    try:
        response = bedrock.invoke_model(
            modelId="global.anthropic.claude-sonnet-4-20250514-v1:0",
            contentType="application/json",
            body=json.dumps(request_body)
        )

        response_body = json.loads(response['body'].read())

        if 'content' in response_body and len(response_body['content']) > 0:
            response_text = response_body['content'][0].get('text', '')

            try:
                json_start = response_text.find('{')
                json_end = response_text.rfind('}') + 1
                if json_start != -1 and json_end > json_start:
                    json_str = response_text[json_start:json_end]
                    analysis_result = json.loads(json_str)
                else:
                    analysis_result = {"error": "Could not extract JSON from response", "raw_response": response_text}
            except json.JSONDecodeError:
                analysis_result = {"error": "Failed to parse JSON", "raw_response": response_text}

            # Save base64 encoded versions of selected images for video generation
            if 'selected_images' in analysis_result:
                product_images_path = Path(folder_path) / 'product_images'
                for img_info in analysis_result['selected_images']:
                    filename = img_info.get('filename', '')
                    if filename:
                        img_path = product_images_path / filename
                        if img_path.exists():
                            try:
                                # Encode the selected image for video generation
                                img = Image.open(img_path)
                                if img.mode == 'RGBA':
                                    img = img.convert('RGB')

                                # Resize for optimal Luma performance (max 2048x2048)
                                img = resize_image_if_needed(img, max_dimension=2048)

                                buffer = io.BytesIO()
                                img.save(buffer, format='JPEG', quality=90)
                                img_base64 = base64.b64encode(buffer.getvalue()).decode('utf-8')

                                # Store the base64 data for video generation
                                img_info['base64_data'] = img_base64
                                print(f"  Encoded {filename} for video generation")
                            except Exception as e:
                                print(f"  Warning: Could not encode {filename}: {str(e)}")

            output_file = Path(folder_path) / 'selected_images.json'
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(analysis_result, f, indent=2, ensure_ascii=False)

            print(f"\nAnalysis complete! Results saved to {output_file}")

            if 'selected_images' in analysis_result:
                print(f"\nSelected {len(analysis_result['selected_images'])} images:")
                for img in analysis_result['selected_images']:
                    print(f"  - {img.get('filename', 'Unknown')}: {img.get('reason', 'No reason provided')}")

            return analysis_result
        else:
            raise ValueError("Unexpected response format from Bedrock")

    except Exception as e:
        print(f"Error during image analysis: {str(e)}")
        raise

def main():
    parser = argparse.ArgumentParser(description='Analyze and select product images for video ad generation')
    parser.add_argument('folder', type=str, help='Path to the folder containing product_images and content_images')
    parser.add_argument('--num-images', '-n', type=int, default=1, help='The number of images to select')

    args = parser.parse_args()

    try:
        _ = analyze_and_select_images(folder_path=args.folder, N=args.num_images)
        print("\nImage selection completed successfully!")
    except Exception as e:
        print(f"Error: {str(e)}")
        exit(1)

if __name__ == "__main__":
    main()