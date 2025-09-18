#!/usr/bin/env python3
import argparse
import boto3
import json
import time
import uuid
import base64
import io
from pathlib import Path
from datetime import datetime
from PIL import Image

def load_prompts(folder_path):
    """Load the per-image prompts from product analysis"""
    prompts_file = Path(folder_path) / 'product_analysis_prompts.json'

    if not prompts_file.exists():
        raise ValueError(f"Product analysis file not found. Please run product_analysis.py first.")

    with open(prompts_file, 'r', encoding='utf-8') as f:
        data = json.load(f)

    # Check for the new per-image format
    if 'per_image_analysis' in data:
        return data
    # Fallback for old format
    elif 'video_prompts' in data:
        print("⚠ Using old prompt format. Re-run product_analysis.py for better results.")
        return {'per_image_analysis': [{'video_prompts': data['video_prompts']}]}
    else:
        raise ValueError("No video prompts found in the analysis file")

def encode_image_for_luma(image_path, max_width=1552, max_height=1552):
    """Encode image for Luma keyframe input

    Luma Ray 2 has specific requirements:
    - Max width: 1552px
    - Max height: 1552px (assumed same as width)
    - Recommended: 512x512 to 1552x1552
    """
    try:
        img = Image.open(image_path)

        # Convert RGBA to RGB if needed
        if img.mode == 'RGBA':
            img = img.convert('RGB')

        # Resize if needed - Luma has max 1552px width constraint
        width, height = img.size
        if width > max_width or height > max_height:
            scale_factor = min(max_width / width, max_height / height)
            new_width = int(width * scale_factor)
            new_height = int(height * scale_factor)
            img = img.resize((new_width, new_height), Image.Resampling.LANCZOS)
            print(f"      Resized image from {width}x{height} to {new_width}x{new_height}")

        # Convert to base64
        buffer = io.BytesIO()
        img.save(buffer, format='JPEG', quality=90)
        img_base64 = base64.b64encode(buffer.getvalue()).decode('utf-8')

        return img_base64
    except Exception as e:
        print(f"Error encoding image {image_path}: {str(e)}")
        return None

def generate_video_async(bedrock_client, prompt_text, s3_bucket, s3_prefix, video_index, keyframe_image=None, image_filename=None):
    print(f"\n🎬 Generating video {video_index}")
    if image_filename:
        print(f"  📸 Source image: {image_filename}")
    print(f"  💬 Prompt: '{prompt_text}'")
    if keyframe_image:
        print(f"  🖼️ Using keyframe from source image")

    # Create unique S3 path for this video
    job_id = str(uuid.uuid4())
    s3_output_uri = f"s3://{s3_bucket}/{s3_prefix}/{job_id}/"

    # Configure the model input (dict format, not bytes)
    model_input = {
        "prompt": prompt_text,
        "aspect_ratio": "16:9",
        "loop": False,
        "duration": "5s",
        "resolution": "720p"
    }

    # Add keyframe if image is provided
    if keyframe_image:
        model_input["keyframes"] = {
            "frame0": {
                "type": "image",
                "source": {
                    "type": "base64",
                    "media_type": "image/jpeg",
                    "data": keyframe_image
                }
            }
        }

    # Configure the output location
    output_config = {
        "s3OutputDataConfig": {
            "s3Uri": s3_output_uri
        }
    }

    try:
        start_time = time.time()

        # Start the async video generation job
        response = bedrock_client.start_async_invoke(
            modelId='luma.ray-v2:0',
            modelInput=model_input,
            outputDataConfig=output_config
        )

        invocation_arn = response['invocationArn']
        print(f"  ⚡ Started async job: {invocation_arn}")
        print(f"  📁 Output will be saved to: {s3_output_uri}")

        # Poll for job completion
        max_attempts = 60  # 10 minutes max
        for attempt in range(max_attempts):
            time.sleep(10)  # Wait 10 seconds between checks

            status_response = bedrock_client.get_async_invoke(
                invocationArn=invocation_arn
            )

            status = status_response['status']
            print(f"    ⏳ Attempt {attempt + 1}/{max_attempts}: Status = {status}")

            if status == 'Completed':
                elapsed_time = time.time() - start_time
                output_location = status_response['outputDataConfig']['s3OutputDataConfig']['s3Uri']

                print(f"  ✅ Video {video_index} generated successfully in {elapsed_time:.1f}s")
                print(f"  📍 S3 Location: {output_location}")

                return {
                    "success": True,
                    "s3_location": output_location,
                    "prompt": prompt_text,
                    "source_image": image_filename,
                    "generation_time": elapsed_time,
                    "job_id": job_id,
                    "video_index": video_index
                }

            elif status == 'Failed':
                # Get detailed error information
                error_msg = status_response.get('failureReason', 'No failureReason provided')

                # Log the entire response for debugging
                print(f"  ❌ Generation failed")
                print(f"     Error message: {error_msg}")
                print(f"     Full response for debugging:")
                print(f"     {json.dumps(status_response, indent=6, default=str)}")

                # Check for common error patterns
                if 'throttling' in error_msg.lower() or 'rate' in error_msg.lower():
                    print(f"     💡 Tip: This might be a rate limiting issue. Try reducing concurrent requests.")
                elif 'permission' in error_msg.lower() or 'access' in error_msg.lower():
                    print(f"     💡 Tip: Check your AWS permissions for Bedrock and S3 access.")
                elif 'quota' in error_msg.lower():
                    print(f"     💡 Tip: You might have reached your AWS Bedrock quota limit.")

                return {
                    "success": False,
                    "error": f"Generation failed: {error_msg}",
                    "prompt": prompt_text,
                    "source_image": image_filename,
                    "video_index": video_index,
                    "full_error_response": status_response
                }

        # Timeout after max attempts
        return {
            "success": False,
            "error": "Generation timed out after maximum attempts",
            "prompt": prompt_text,
            "source_image": image_filename,
            "video_index": video_index
        }

    except Exception as e:
        return {
            "success": False,
            "error": f"Error generating video: {str(e)}",
            "prompt": prompt_text,
            "source_image": image_filename,
            "video_index": video_index
        }

def download_video_from_s3(s3_client, s3_location, local_path):
    """Download video from S3 to local path"""
    try:
        # Parse S3 URI
        s3_uri = s3_location.rstrip('/')
        if s3_uri.startswith('s3://'):
            s3_uri = s3_uri[5:]

        parts = s3_uri.split('/', 1)
        if len(parts) != 2:
            return False

        bucket = parts[0]
        prefix = parts[1]

        # List objects in the prefix
        response = s3_client.list_objects_v2(
            Bucket=bucket,
            Prefix=prefix
        )

        if 'Contents' in response:
            # Find the video file (should be the only .mp4 file)
            for obj in response['Contents']:
                if obj['Key'].endswith('.mp4'):
                    print(f"    📥 Downloading {obj['Key']} to {local_path}")
                    s3_client.download_file(bucket, obj['Key'], str(local_path))
                    return True

        return False

    except Exception as e:
        print(f"    ❌ Error downloading from S3: {str(e)}")
        return False

def generate_ads(folder_path, num_videos_per_image=3, s3_bucket=None, use_images=True, session_timestamp=None):
    bedrock = boto3.client(
        service_name='bedrock-runtime',
        region_name='us-west-2'
    )

    s3 = boto3.client('s3', region_name='us-west-2')

    # If no S3 bucket provided, try to use default or ask user
    if not s3_bucket:
        print("\n⚠️ WARNING: Luma Ray2 requires an S3 bucket for output.")
        print("Please provide an S3 bucket name using --s3-bucket parameter")
        print("Example: python ads_generation.py ./folder --s3-bucket my-bucket-name")
        return None

    analysis_data = load_prompts(folder_path)

    # Handle new per-image format
    if 'per_image_analysis' in analysis_data:
        per_image_analyses = analysis_data['per_image_analysis']
        print(f"\n📊 Found analysis for {len(per_image_analyses)} images")
    else:
        print("\n⚠️ Old format detected. Using all prompts for single generation.")
        per_image_analyses = [{'video_prompts': analysis_data.get('video_prompts', [])}]

    product_images_path = Path(folder_path) / 'product_images'

    # Create output folder for generated videos
    generated_folder = Path(folder_path) / 'generated_ads'
    generated_folder.mkdir(exist_ok=True)

    # Use provided timestamp or generate new one
    if session_timestamp:
        timestamp = session_timestamp
    else:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

    s3_prefix = f"luma-videos/{timestamp}"

    successful_videos = []
    failed_videos = []
    total_video_index = 0

    print(f"\n🎯 Starting video generation")
    print(f"  • Session timestamp: {timestamp}")
    print(f"  • Images to process: {len(per_image_analyses)}")
    print(f"  • Videos per image: {num_videos_per_image}")
    print(f"  • Total videos to generate: {len(per_image_analyses) * num_videos_per_image}")
    print(f"  • S3 bucket: {s3_bucket}")
    print(f"  • S3 prefix: {s3_prefix}")

    # Process each image's prompts
    for img_idx, image_analysis in enumerate(per_image_analyses, 1):
        image_filename = image_analysis.get('image_filename', f'image_{img_idx}')
        video_prompts = image_analysis.get('video_prompts', [])

        if not video_prompts:
            print(f"\n⚠️ No prompts found for {image_filename}, skipping...")
            continue

        print(f"\n{'='*60}")
        print(f"📸 Processing Image {img_idx}/{len(per_image_analyses)}: {image_filename}")
        print(f"   Found {len(video_prompts)} prompts")

        # Load and encode the image for keyframe
        keyframe_image = None
        if use_images and image_filename != f'image_{img_idx}':  # Only if we have real filename
            img_path = product_images_path / image_filename
            if img_path.exists():
                print(f"   🖼️ Encoding image for keyframe use...")
                keyframe_image = encode_image_for_luma(img_path)
            else:
                print(f"   ⚠️ Image file not found: {img_path}")

        # Generate videos for this image (up to num_videos_per_image)
        prompts_to_use = video_prompts[:num_videos_per_image]

        for prompt_idx, prompt_data in enumerate(prompts_to_use, 1):
            total_video_index += 1

            # Extract prompt text
            if isinstance(prompt_data, dict):
                prompt_text = prompt_data.get('prompt', '')
                prompt_type = prompt_data.get('prompt_type', 'Unknown')
            else:
                prompt_text = str(prompt_data)
                prompt_type = 'Unknown'

            print(f"\n   🎬 Video {prompt_idx}/{len(prompts_to_use)} - {prompt_type}")
            print(f"      Prompt: {prompt_text[:100]}...")

            # Generate video
            result = generate_video_async(
                bedrock,
                prompt_text,
                s3_bucket,
                s3_prefix,
                total_video_index,
                keyframe_image=keyframe_image,
                image_filename=image_filename
            )

            # Add source image info to result
            result['source_image'] = image_filename
            result['prompt_type'] = prompt_type
            result['image_index'] = img_idx
            result['prompt_index'] = prompt_idx
            result['timestamp'] = timestamp

            if result['success']:
                # Download the video locally with timestamp in filename
                local_filename = f"video_{timestamp}_{img_idx:02d}_{prompt_idx:02d}_{prompt_type.lower().replace(' ', '_')}.mp4"
                local_path = generated_folder / local_filename

                print(f"   📥 Downloading to {local_filename}...")
                if download_video_from_s3(s3, result['s3_location'], local_path):
                    result['local_filename'] = str(local_path)
                    print(f"   ✅ Saved locally: {local_filename}")

                successful_videos.append(result)
            else:
                failed_videos.append(result)

    print(f"\n{'='*60}")
    print(f"📊 Generation Summary:")
    print(f"  ✅ Successful: {len(successful_videos)} videos")
    print(f"  ❌ Failed: {len(failed_videos)} videos")

    # Create generation report
    report = {
        "timestamp": timestamp,
        "timestamp_iso": datetime.now().isoformat(),
        "s3_bucket": s3_bucket,
        "s3_prefix": s3_prefix,
        "total_images_processed": len(per_image_analyses),
        "videos_per_image": num_videos_per_image,
        "total_attempted": total_video_index,
        "successful_count": len(successful_videos),
        "failed_count": len(failed_videos),
        "successful_videos": successful_videos,
        "failed_videos": failed_videos,
        "generation_details": {
            "use_keyframes": use_images,
            "aspect_ratio": "16:9",
            "duration": "5s",
            "resolution": "720p"
        }
    }

    # Save generation report
    report_file = generated_folder / f"generation_report_{timestamp}.json"
    with open(report_file, 'w', encoding='utf-8') as f:
        # Use default=str to handle datetime and other non-serializable objects
        json.dump(report, f, indent=2, default=str)

    print(f"\n📄 Generation report saved: {report_file}")

    # Create list of successfully generated videos for session-aware merging
    session_videos = []
    for video in successful_videos:
        if 'local_filename' in video:
            session_videos.append(Path(video['local_filename']).name)

    # Save session-specific video list for merging
    if session_videos:
        session_file = generated_folder / f'session_videos_{timestamp}.json'
        session_data = {
            "timestamp": timestamp,
            "timestamp_iso": datetime.now().isoformat(),
            "session_videos": session_videos,
            "generation_report": str(report_file.name)
        }
        with open(session_file, 'w', encoding='utf-8') as f:
            json.dump(session_data, f, indent=2)
        print(f"📝 Session video list saved: {session_file}")

        # Also save as latest session for easy access
        latest_session_file = generated_folder / 'latest_session_videos.json'
        with open(latest_session_file, 'w', encoding='utf-8') as f:
            json.dump(session_data, f, indent=2)
        print(f"📝 Latest session link saved: {latest_session_file}")

    # Display per-image summary
    print(f"\n📸 Per-Image Summary:")
    current_image = None
    for video in successful_videos:
        if video.get('source_image') != current_image:
            current_image = video.get('source_image')
            print(f"\n  {current_image}:")
        print(f"    • {video.get('prompt_type', 'Unknown')}: ✅ Generated")

    return report

def main():
    parser = argparse.ArgumentParser(description='Generate video advertisements using Luma Ray 2')
    parser.add_argument('folder', type=str, help='Path to the folder containing product analysis')
    parser.add_argument('--num-videos', '-n', type=int, default=3,
                       help='Number of videos to generate per image (default: 3)')
    parser.add_argument('--s3-bucket', type=str,
                       help='S3 bucket name for video output (required)')
    parser.add_argument('--no-images', action='store_true',
                       help='Disable using product images as keyframes')
    parser.add_argument('--timestamp', type=str,
                       help='Use specific timestamp for session (format: YYYYMMDD_HHMMSS)')
    args = parser.parse_args()

    try:
        use_images = not args.no_images
        result = generate_ads(args.folder, args.num_videos, args.s3_bucket, use_images, args.timestamp)
        if result:
            print(f"\n✅ Video generation completed successfully!")
            print(f"   Session: {result['timestamp']}")
            print(f"   Total videos generated: {result['successful_count']}")
    except Exception as e:
        print(f"\n❌ Error: {str(e)}")
        exit(1)

if __name__ == "__main__":
    main()