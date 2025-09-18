#!/usr/bin/env python3
import argparse
import boto3
import json
import base64
from pathlib import Path
from PIL import Image
import io
from datetime import datetime

def load_selected_images(folder_path):
    selected_images_file = Path(folder_path) / 'selected_images.json'

    if not selected_images_file.exists():
        raise ValueError(f"Selected images file not found. Please run image_analysis_and_selection.py first.")

    with open(selected_images_file, 'r', encoding='utf-8') as f:
        return json.load(f)

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

def encode_image_for_bedrock(image_path):
    img = Image.open(image_path)
    if img.mode == 'RGBA':
        img = img.convert('RGB')

    # Resize if needed to stay within max dimension limit
    img = resize_image_if_needed(img, max_dimension=7500)

    buffer = io.BytesIO()
    img.save(buffer, format='JPEG', quality=85)
    return base64.b64encode(buffer.getvalue()).decode('utf-8')

def analyze_single_image_and_generate_prompts(image_path, image_info, num_prompts=3, content_images_path=None):
    """Generate prompts for a single product image"""
    bedrock = boto3.client(
        service_name='bedrock-runtime',
        region_name='us-west-2'
    )

    messages = [
        {
            "role": "user",
            "content": []
        }
    ]

    content_parts = []

    content_parts.append({
        "type": "text",
        "text": f"""You are a prompt generator for Luma Ray 2 (video generation AI) specialized in creating product advertisement videos.

Analyze this specific product image and generate EXACTLY {num_prompts} optimized video prompts following this EXACT structure:

[Camera type/shot], [Main subject], [Subject action], [Camera movement], [Lighting], [Mood]

**CRITICAL REQUIREMENTS:**
- Maximum 3-4 sentences per prompt
- Include specific camera movements (tracking shot, dolly zoom, crane shot, orbital pan, etc.)
- Use premium lighting descriptions (studio lighting, golden hour, soft ambient, rim lighting, dramatic shadows, etc.)
- Apply commercial mood keywords (premium, sophisticated, sleek, innovative, luxurious, etc.)
- Ensure immediate usability in Luma Ray 2
- Each prompt should be uniquely tailored to THIS specific product image

Generate new prompts following this EXACT format in JSON:

{{
    "image_filename": "{image_info.get('filename', 'unknown')}",
    "product_analysis": {{
        "product_identification": "Brief product identification for this specific image",
        "key_features": ["feature1", "feature2", "feature3"],
        "image_specific_details": "What makes this particular image/angle unique",
        "visual_style": "Premium commercial style for this shot"
    }},
    "video_prompts": [
        {{
            "sequence": 1,
            "prompt_type": "Hero Showcase",
            "prompt": "Wide shot, [product] centered on pristine white surface, slowly dolly zoom, tracking camera movement following dolly zoom. Studio lighting with soft shadows, highlighting product details. Premium, sophisticated mood with sleek presentation.",
            "camera_movement": "specific movement type",
            "lighting": "specific lighting setup",
            "mood": "specific mood"
        }},
        {{
            "sequence": 2,
            "prompt_type": "Lifestyle Focus",
            "prompt": "Medium shot, [product] being used in elegant lifestyle setting, smooth dolly forward revealing product details, camera transitions from wide to close-up. Natural golden hour lighting with warm tones. Aspirational, inviting mood showcasing lifestyle integration.",
            "camera_movement": "specific movement type",
            "lighting": "specific lighting setup",
            "mood": "specific mood"
        }},
        {{
            "sequence": 3,
            "prompt_type": "Technical Detail",
            "prompt": "Macro shot, [product] features in extreme detail, precise orbital camera movement around key features, slow-motion capture of textures. Professional rim lighting emphasizing materials and craftsmanship. Innovative, high-tech mood with focus on quality.",
            "camera_movement": "specific movement type",
            "lighting": "specific lighting setup",
            "mood": "specific mood"
        }}
    ],
    "story_summary": "Overall commercial narrative arc for this specific image"
}}

**Camera Movement Options:** tracking shot, dolly zoom, crane shot, orbital pan, slider movement, handheld stabilized, smooth zoom, pull focus transition

**Lighting Options:** studio lighting, golden hour, soft ambient, rim lighting, dramatic shadows, backlit silhouette, product photography lighting, cinematic lighting, natural daylight

**Mood Keywords:** premium, sophisticated, sleek, innovative, luxurious, aspirational, dynamic, elegant, modern, cutting-edge

Remember: Focus on THIS SPECIFIC IMAGE and its unique characteristics when generating prompts. DO NOT MODIFY THE ORIGINAL IMAGE"""
    })

    # Add the specific product image
    print(f"  Processing: {image_info.get('filename', 'unknown')}")
    img_base64 = encode_image_for_bedrock(image_path)

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
        "text": f"Product image: {image_info.get('filename', 'unknown')}\nReason selected: {image_info.get('reason', 'N/A')}\nVisual features: {image_info.get('visual_features', 'N/A')}"
    })

    # Add content images if available
    if content_images_path and content_images_path.exists():
        content_files = list(content_images_path.glob('*.jpg')) + \
                       list(content_images_path.glob('*.jpeg')) + \
                       list(content_images_path.glob('*.png'))

        if content_files:
            content_parts.append({
                "type": "text",
                "text": "\n--- Additional Content Images for Context ---"
            })

            for content_img_path in content_files[:3]:  # Limit to 3 content images per request
                img_base64 = encode_image_for_bedrock(content_img_path)

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
                    "text": f"Context image: {content_img_path.name}"
                })

    messages[0]["content"] = content_parts

    request_body = {
        "anthropic_version": "bedrock-2023-05-31",
        "messages": messages,
        "system": "You are a Luma Ray 2 prompt specialist. Generate premium product advertisement video prompts with specific camera movements, professional lighting, and commercial moods. Each prompt must be 3-4 sentences following the exact structure: [Camera/shot], [Subject], [Action], [Movement], [Lighting], [Mood].",
        "max_tokens": 4096,
        "temperature": 0.7
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

            return analysis_result
        else:
            raise ValueError("Unexpected response format from Bedrock")

    except Exception as e:
        print(f"    Error analyzing image: {str(e)}")
        raise

def analyze_product_and_generate_prompts(folder_path, num_prompts=3):
    """Analyze each selected image individually and generate prompts for each"""

    selected_images = load_selected_images(folder_path)

    if 'selected_images' not in selected_images:
        raise ValueError("No selected images found in the analysis file")

    product_images_path = Path(folder_path) / 'product_images'
    content_images_path = Path(folder_path) / 'content_images'

    all_analyses = []

    print(f"\nAnalyzing {len(selected_images['selected_images'])} selected images...")
    print(f"Generating {num_prompts} prompts per image...")

    # Analyze each selected image individually
    for idx, selected_img in enumerate(selected_images['selected_images'], 1):
        img_filename = selected_img.get('filename')
        if img_filename:
            img_path = product_images_path / img_filename
            if img_path.exists():
                print(f"\n[{idx}/{len(selected_images['selected_images'])}] Analyzing {img_filename}...")

                try:
                    analysis = analyze_single_image_and_generate_prompts(
                        img_path,
                        selected_img,
                        num_prompts,
                        content_images_path
                    )

                    # Add metadata
                    analysis['image_index'] = idx
                    analysis['total_images'] = len(selected_images['selected_images'])

                    all_analyses.append(analysis)

                    if 'video_prompts' in analysis:
                        print(f"  ✓ Generated {len(analysis['video_prompts'])} prompts for {img_filename}")

                except Exception as e:
                    print(f"  ✗ Failed to analyze {img_filename}: {str(e)}")
                    all_analyses.append({
                        'image_filename': img_filename,
                        'image_index': idx,
                        'error': str(e)
                    })
            else:
                print(f"  ✗ Image file not found: {img_path}")

    # Compile final result
    timestamp = datetime.now().isoformat()
    final_result = {
        'metadata': {
            'timestamp': timestamp,
            'num_prompts_per_image': num_prompts,
            'total_images_analyzed': len(all_analyses),
            'total_prompts_generated': sum(
                len(a.get('video_prompts', [])) for a in all_analyses
            ),
            'prompt_format': 'Luma Ray 2 optimized'
        },
        'per_image_analysis': all_analyses,
        'generation_summary': {
            'successful_analyses': sum(1 for a in all_analyses if 'video_prompts' in a),
            'failed_analyses': sum(1 for a in all_analyses if 'error' in a),
            'total_video_prompts': sum(
                len(a.get('video_prompts', [])) for a in all_analyses
            )
        }
    }

    # Save with timestamp in filename
    analysis_folder = Path(folder_path) / 'analysis_history'
    analysis_folder.mkdir(exist_ok=True)

    # Save timestamped version
    timestamp_str = datetime.now().strftime('%Y%m%d_%H%M%S')
    timestamped_file = analysis_folder / f'analysis_{timestamp_str}.json'
    with open(timestamped_file, 'w', encoding='utf-8') as f:
        json.dump(final_result, f, indent=2, ensure_ascii=False)

    # Also save as the main file for backward compatibility
    output_file = Path(folder_path) / 'product_analysis_prompts.json'
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(final_result, f, indent=2, ensure_ascii=False)

    print(f"\n{'='*60}")
    print(f"Analysis complete!")
    print(f"  Images analyzed: {final_result['generation_summary']['successful_analyses']}")
    print(f"  Total prompts generated: {final_result['generation_summary']['total_video_prompts']}")
    print(f"  Current version: {output_file}")
    print(f"  Archived version: {timestamped_file}")

    # Display summary for each image
    for analysis in all_analyses:
        if 'video_prompts' in analysis:
            print(f"\n📸 {analysis.get('image_filename', 'Unknown')}:")
            for prompt_data in analysis['video_prompts']:
                prompt_type = prompt_data.get('prompt_type', 'Unknown')
                print(f"  • PROMPT {prompt_data.get('sequence', '?')} - {prompt_type}")

    return final_result

def main():
    parser = argparse.ArgumentParser(description='Generate Luma Ray 2 optimized video advertisement prompts for each selected image')
    parser.add_argument('folder', type=str, help='Path to the folder containing product and content images')
    parser.add_argument('--num-prompts', '-n', type=int, default=3,
                       help='Number of prompts to generate per image (default: 3)')
    args = parser.parse_args()

    try:
        result = analyze_product_and_generate_prompts(args.folder, args.num_prompts)
        print(f"\n✅ Luma Ray 2 prompt generation completed successfully!")
        print(f"   Generated {result['generation_summary']['total_video_prompts']} total prompts")
        print(f"   ({args.num_prompts} prompts × {result['generation_summary']['successful_analyses']} images)")
    except Exception as e:
        print(f"Error: {str(e)}")
        exit(1)

if __name__ == "__main__":
    main()