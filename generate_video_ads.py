#!/usr/bin/env python3
import argparse
import subprocess
import sys
from pathlib import Path
import json
from datetime import datetime

def run_command(command):
    print(f"\n{'='*60}")
    print(f"Running: {' '.join([str(c) for c in command])}")
    print('='*60)

    try:
        result = subprocess.run(command, capture_output=True, text=True, check=True)
        if result.stdout:
            print(result.stdout)
        return True
    except subprocess.CalledProcessError as e:
        print(f"Error: {e}")
        if e.stdout:
            print(f"Output: {e.stdout}")
        if e.stderr:
            print(f"Error output: {e.stderr}")
        return False

def validate_folder_structure(folder_path):
    folder = Path(folder_path)

    if not folder.exists():
        print(f"Error: Folder '{folder_path}' does not exist")
        return False

    product_images = folder / 'product_images'
    content_images = folder / 'content_images'

    if not product_images.exists():
        print(f"Error: 'product_images' folder not found in {folder_path}")
        return False

    product_image_count = len(list(product_images.glob('*.jpg')) +
                             list(product_images.glob('*.jpeg')) +
                             list(product_images.glob('*.png')))

    if product_image_count == 0:
        print(f"Error: No images found in 'product_images' folder")
        return False

    print(f"✓ Found {product_image_count} product images")

    if content_images.exists():
        content_image_count = len(list(content_images.glob('*.jpg')) +
                                 list(content_images.glob('*.jpeg')) +
                                 list(content_images.glob('*.png')))
        print(f"✓ Found {content_image_count} content images")
    else:
        print("ℹ 'content_images' folder not found (optional)")

    return True

def get_latest_session_timestamp(folder_path):
    """Get the timestamp from the latest video generation session"""
    generated_folder = Path(folder_path) / 'generated_ads'

    if generated_folder.exists():
        # Try to read from latest_session_videos.json
        latest_session_file = generated_folder / 'latest_session_videos.json'
        if latest_session_file.exists():
            try:
                with open(latest_session_file, 'r', encoding='utf-8') as f:
                    session_data = json.load(f)
                    return session_data.get('timestamp')
            except Exception as e:
                print(f"⚠ Error reading latest session file: {e}")

        # Fallback: Find the most recent session file
        session_files = list(generated_folder.glob('session_videos_*.json'))
        if session_files:
            session_files.sort()
            latest_file = session_files[-1]
            # Extract timestamp from filename
            filename = latest_file.stem  # session_videos_YYYYMMDD_HHMMSS
            if '_' in filename:
                parts = filename.split('_')
                if len(parts) >= 4:
                    return f"{parts[2]}_{parts[3]}"

    return None

def main():
    parser = argparse.ArgumentParser(
        description='Generate video ads from product images using Amazon Bedrock and Luma Ray 2',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Example usage:
  # Basic usage - select 2 images, generate 3 prompts per image = 6 total videos
  python generate_video_ads.py ./my_product --num-images 2 --prompts 3 --s3-bucket my-bucket

  # Quick test - 1 image, 2 prompts = 2 total videos
  python generate_video_ads.py ./my_product --num-images 1 --prompts 2 --s3-bucket my-bucket

  # Full pipeline with merging - 3 images, 3 prompts = 9 total videos
  python generate_video_ads.py ./my_product --num-images 3 --prompts 3 --s3-bucket my-bucket --merge

The folder structure should be:
  my_product/
    ├── product_images/   (required)
    │   ├── image1.jpg
    │   ├── image2.png
    │   └── ...
    └── content_images/   (optional)
        ├── content1.jpg
        └── ...

Total videos generated = num-images × prompts
(e.g., 2 images × 3 prompts = 6 total videos)
        """
    )

    parser.add_argument('folder', type=str,
                       help='Path to the folder containing product_images and content_images')
    parser.add_argument('--num-images', '-n', type=int, default=1,
                       help='Number of images to select for processing (default: 1)')
    parser.add_argument('--prompts', '-p', type=int, default=3,
                       help='Number of prompts to generate per image (default: 3)')
    parser.add_argument('--skip-selection', action='store_true',
                       help='Skip image selection if already done')
    parser.add_argument('--skip-analysis', action='store_true',
                       help='Skip product analysis if already done')
    parser.add_argument('--s3-bucket', type=str,
                       help='S3 bucket name for video output (required for video generation)')
    parser.add_argument('--merge', action='store_true',
                       help='Merge generated videos into a single video (current session only)')
    parser.add_argument('--merge-all', action='store_true',
                       help='Merge all videos in directory (not just current session)')
    parser.add_argument('--transition', action='store_true',
                       help='Add fade transitions when merging videos')

    args = parser.parse_args()

    # Calculate total videos automatically
    total_videos = args.num_images * args.prompts

    print("\n" + "="*60)
    print("🎬 Video Ad Generation Pipeline - Luma Ray 2")
    print("="*60)
    print(f"📁 Folder: {args.folder}")
    print(f"🖼️  Images to select: {args.num_images}")
    print(f"📝 Prompts per image: {args.prompts}")
    print(f"🎥 Videos per image: {args.prompts} (one per prompt)")
    print(f"📊 Total videos: {args.num_images} × {args.prompts} = {total_videos}")

    if not validate_folder_structure(args.folder):
        sys.exit(1)

    steps = []

    # Generate a session timestamp for this pipeline run
    session_timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    print(f"📅 Session timestamp: {session_timestamp}")

    if not args.skip_selection:
        steps.append({
            'name': f'Image Selection ({args.num_images} images)',
            'command': ['python', 'image_analysis_and_selection.py', args.folder, '--num-images', str(args.num_images)]
        })
    else:
        print("\nℹ Skipping image selection (--skip-selection flag)")

    if not args.skip_analysis:
        steps.append({
            'name': f'Product Analysis & Prompt Generation ({args.prompts} prompts per image)',
            'command': ['python', 'product_analysis.py', args.folder, '-n', str(args.prompts)]
        })
    else:
        print("\nℹ Skipping product analysis (--skip-analysis flag)")

    generated_folder = Path(args.folder) / 'generated_ads'

    if args.s3_bucket:
        # Add timestamp to video generation command
        video_gen_command = ['python', 'ads_generation.py', args.folder, '-n', str(args.prompts),
                            '--s3-bucket', args.s3_bucket, '--timestamp', session_timestamp]

        steps.append({
            'name': f'Video Generation ({args.prompts} videos per image, {total_videos} total)',
            'command': video_gen_command
        })

        # Add merge step if requested
        if args.merge or args.merge_all:
            merge_cmd = ['python', 'merge_videos.py', str(generated_folder)]

            if args.transition:
                merge_cmd.append('--transition')

            if args.merge_all:
                merge_cmd.append('--all-videos')
                merge_name = 'Video Merging (All Videos)'
            else:
                # Use the session timestamp for merging
                merge_cmd.extend(['--timestamp', session_timestamp])
                merge_name = f'Video Merging (Session {session_timestamp})'

            steps.append({
                'name': merge_name,
                'command': merge_cmd
            })
    else:
        print("\n⚠️  WARNING: S3 bucket not provided. Video generation will be skipped.")
        print("   To generate videos, provide --s3-bucket parameter")
        print(f"   Example: python generate_video_ads.py {args.folder} --s3-bucket your-bucket-name")

    print(f"\n{'='*60}")
    print("🚀 Starting pipeline...")
    print('='*60)

    for i, step in enumerate(steps, 1):
        print(f"\n[Step {i}/{len(steps)}] {step['name']}")

        if not run_command(step['command']):
            print(f"\n❌ Pipeline failed at step: {step['name']}")
            sys.exit(1)

        print(f"✅ {step['name']} completed successfully")

    print("\n" + "="*60)
    print("✅ Pipeline completed successfully!")
    print("="*60)

    # Check for generated videos
    if generated_folder.exists():
        # Look for videos with the session timestamp
        session_videos = list(generated_folder.glob(f'video_{session_timestamp}_*.mp4'))
        all_videos = list(generated_folder.glob('video_*.mp4'))
        merged_videos = list(generated_folder.glob('merged_*.mp4'))

        if session_videos:
            print(f"\n🎥 Generated {len(session_videos)} video(s) in this session:")
            # Group by source image
            videos_by_image = {}
            for video in session_videos:
                # Extract image index from filename (video_TIMESTAMP_01_02_hero_showcase.mp4)
                parts = video.stem.split('_')
                if len(parts) >= 4:
                    img_idx = parts[2]  # After timestamp
                    if img_idx not in videos_by_image:
                        videos_by_image[img_idx] = []
                    videos_by_image[img_idx].append(video.name)

            for img_idx in sorted(videos_by_image.keys()):
                print(f"\n  Image {img_idx}:")
                for video_name in sorted(videos_by_image[img_idx]):
                    print(f"    • {video_name}")

        elif all_videos:
            print(f"\n🎥 Found {len(all_videos)} existing video(s) in folder")

        if merged_videos:
            # Find the most recent merged video
            latest_merged = None
            for video in merged_videos:
                if session_timestamp in video.name:
                    latest_merged = video
                    break

            if latest_merged:
                print(f"\n🎬 Session merged video:")
                print(f"  • {latest_merged.name}")
            elif merged_videos:
                print(f"\n🎬 Existing merged video(s):")
                for video in merged_videos[-3:]:  # Show last 3
                    print(f"  • {video.name}")

    # Display file locations
    selected_images_file = Path(args.folder) / 'selected_images.json'
    prompts_file = Path(args.folder) / 'product_analysis_prompts.json'

    print("\n📂 Generated files:")
    if selected_images_file.exists():
        with open(selected_images_file, 'r') as f:
            data = json.load(f)
            num_selected = len(data.get('selected_images', []))
        print(f"  • Image selection: {selected_images_file} ({num_selected} images)")

    if prompts_file.exists():
        with open(prompts_file, 'r') as f:
            data = json.load(f)
            if 'per_image_analysis' in data:
                num_prompts = data['generation_summary']['total_video_prompts']
                print(f"  • Product analysis: {prompts_file} ({num_prompts} total prompts)")
            else:
                print(f"  • Product analysis: {prompts_file}")

    if generated_folder.exists():
        print(f"  • Videos folder: {generated_folder}")

        # Look for session-specific files
        session_file = generated_folder / f'session_videos_{session_timestamp}.json'
        if session_file.exists():
            with open(session_file, 'r') as f:
                session_data = json.load(f)
            print(f"  • Session file: {session_file} ({len(session_data['session_videos'])} videos)")

        # Look for generation report
        report_file = generated_folder / f'generation_report_{session_timestamp}.json'
        if report_file.exists():
            with open(report_file, 'r') as f:
                report_data = json.load(f)
            print(f"  • Generation report: {report_file}")
            print(f"    - Successful: {report_data['successful_count']} videos")
            print(f"    - Failed: {report_data['failed_count']} videos")

    print(f"\n📅 Session ID: {session_timestamp}")
    print("   Use this timestamp to merge or reference videos from this session")

if __name__ == "__main__":
    main()