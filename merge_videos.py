#!/usr/bin/env python3
"""Merge multiple video files into a single video with timestamp-based selection"""
import argparse
import subprocess
from pathlib import Path
from datetime import datetime
import json
import sys

def check_ffmpeg():
    """Check if ffmpeg is installed"""
    try:
        result = subprocess.run(['ffmpeg', '-version'], capture_output=True, text=True)
        if result.returncode == 0:
            print("✓ ffmpeg is installed")
            return True
    except FileNotFoundError:
        pass

    print("✗ ffmpeg is not installed")
    print("\nTo install ffmpeg:")
    print("  macOS: brew install ffmpeg")
    print("  Ubuntu: sudo apt-get install ffmpeg")
    print("  Windows: Download from https://ffmpeg.org/download.html")
    return False

def get_video_info(video_path):
    """Get video information using ffprobe"""
    try:
        cmd = [
            'ffprobe', '-v', 'quiet',
            '-print_format', 'json',
            '-show_streams',
            str(video_path)
        ]
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode == 0:
            info = json.loads(result.stdout)
            for stream in info.get('streams', []):
                if stream['codec_type'] == 'video':
                    return {
                        'width': stream.get('width', 0),
                        'height': stream.get('height', 0),
                        'duration': float(stream.get('duration', 0))
                    }
    except Exception as e:
        print(f"Error getting video info: {e}")
    return None

def create_concat_file(video_files, output_dir):
    """Create a concat file for ffmpeg"""
    concat_file = output_dir / 'concat_list.txt'
    with open(concat_file, 'w') as f:
        for video in video_files:
            # Use absolute path to avoid issues
            f.write(f"file '{video.absolute()}'\n")
    return concat_file

def merge_videos_concat(video_files, output_path, transition='none'):
    """Merge videos using ffmpeg concat demuxer (fast, no re-encoding)"""
    print(f"\nMerging {len(video_files)} videos using concat method...")

    # Create concat file
    concat_file = output_path.parent / 'concat_list.txt'
    with open(concat_file, 'w') as f:
        for video in video_files:
            f.write(f"file '{video.absolute()}'\n")

    try:
        # Use concat demuxer (fast, no re-encoding)
        cmd = [
            'ffmpeg', '-y',
            '-f', 'concat',
            '-safe', '0',
            '-i', str(concat_file),
            '-c', 'copy',  # Copy codec (no re-encoding)
            str(output_path)
        ]

        print(f"Running: {' '.join(cmd)}")
        result = subprocess.run(cmd, capture_output=True, text=True)

        if result.returncode == 0:
            print(f"✓ Videos merged successfully: {output_path}")
            # Clean up concat file
            concat_file.unlink()
            return True
        else:
            print(f"✗ Error merging videos: {result.stderr}")
            return False

    except Exception as e:
        print(f"✗ Error during merging: {e}")
        return False
    finally:
        # Clean up concat file if it exists
        if concat_file.exists():
            concat_file.unlink()

def merge_videos_with_transitions(video_files, output_path, transition_duration=0.5):
    """Merge videos with fade transitions between them"""
    print(f"\nMerging {len(video_files)} videos with fade transitions...")

    # Build complex filter for transitions
    filter_complex = []

    # First, we need to ensure all videos have the same resolution
    # Get the resolution of the first video
    first_info = get_video_info(video_files[0])
    if not first_info:
        print("✗ Could not get video information")
        return False

    target_width = first_info['width']
    target_height = first_info['height']

    # Scale all videos to the same size and add fade effects
    for i in range(len(video_files)):
        # Scale to target resolution
        filter_complex.append(f"[{i}:v]scale={target_width}:{target_height}:force_original_aspect_ratio=decrease,pad={target_width}:{target_height}:(ow-iw)/2:(oh-ih)/2,setsar=1")

        # Add fade in at the beginning (except first video)
        if i > 0:
            filter_complex[-1] += f",fade=t=in:st=0:d={transition_duration}"

        # Add fade out at the end (except last video)
        if i < len(video_files) - 1:
            # We need to know the duration to set fade out properly
            video_info = get_video_info(video_files[i])
            if video_info and video_info['duration'] > 0:
                fade_start = video_info['duration'] - transition_duration
                filter_complex[-1] += f",fade=t=out:st={fade_start}:d={transition_duration}"

        filter_complex[-1] += f"[v{i}]"

    # Concatenate all processed videos
    concat_filter = ""
    for i in range(len(video_files)):
        concat_filter += f"[v{i}]"
    concat_filter += f"concat=n={len(video_files)}:v=1:a=0[outv]"

    filter_complex.append(concat_filter)

    # Build ffmpeg command
    cmd = ['ffmpeg', '-y']

    # Add input files
    for video in video_files:
        cmd.extend(['-i', str(video)])

    # Add filter complex
    cmd.extend(['-filter_complex', ';'.join(filter_complex)])

    # Output options
    cmd.extend([
        '-map', '[outv]',
        '-c:v', 'libx264',
        '-preset', 'medium',
        '-crf', '23',
        '-pix_fmt', 'yuv420p',
        str(output_path)
    ])

    try:
        print(f"Processing videos with transitions...")
        result = subprocess.run(cmd, capture_output=True, text=True)

        if result.returncode == 0:
            print(f"✓ Videos merged with transitions: {output_path}")
            return True
        else:
            print(f"✗ Error merging videos: {result.stderr}")
            return False

    except Exception as e:
        print(f"✗ Error during merging: {e}")
        return False

def get_latest_session_timestamp(generated_ads_dir):
    """Get the timestamp of the latest video generation session"""

    # First try to read from latest_session_videos.json
    latest_session_file = generated_ads_dir / 'latest_session_videos.json'
    if latest_session_file.exists():
        try:
            with open(latest_session_file, 'r', encoding='utf-8') as f:
                session_data = json.load(f)
                return session_data.get('timestamp')
        except Exception as e:
            print(f"⚠ Error reading latest session file: {e}")

    # Fallback: Find the most recent session file by timestamp
    session_files = list(generated_ads_dir.glob('session_videos_*.json'))
    if session_files:
        # Sort by filename (timestamp is in the name)
        session_files.sort()
        latest_file = session_files[-1]

        # Extract timestamp from filename
        filename = latest_file.stem  # session_videos_YYYYMMDD_HHMMSS
        if '_' in filename:
            parts = filename.split('_')
            if len(parts) >= 4:
                timestamp = f"{parts[2]}_{parts[3]}"
                return timestamp

    return None

def merge_generated_videos(folder_path, transition=False, transition_duration=0.5,
                         session_only=True, use_timestamp=None):
    """Merge generated videos in the folder

    Args:
        folder_path: Path to the folder containing generated_ads
        transition: Whether to add fade transitions
        transition_duration: Duration of transitions in seconds
        session_only: If True, only merge videos from current/specified session
        use_timestamp: Specific timestamp to use for filtering videos
    """

    generated_ads_dir = Path(folder_path)

    if not generated_ads_dir.exists():
        print(f"✗ Generated ads folder not found: {generated_ads_dir}")
        return False

    video_files = []
    session_timestamp = use_timestamp

    if session_only:
        # Determine which timestamp to use
        if not session_timestamp:
            session_timestamp = get_latest_session_timestamp(generated_ads_dir)

        if session_timestamp:
            print(f"📅 Using session timestamp: {session_timestamp}")

            # Look for session file with this timestamp
            session_file = generated_ads_dir / f'session_videos_{session_timestamp}.json'

            if session_file.exists():
                try:
                    with open(session_file, 'r', encoding='utf-8') as f:
                        session_data = json.load(f)

                    session_videos = session_data.get('session_videos', [])
                    print(f"Found session file with {len(session_videos)} videos")

                    # Build full paths for session videos
                    for video_name in session_videos:
                        video_path = generated_ads_dir / video_name
                        if video_path.exists():
                            video_files.append(video_path)
                        else:
                            print(f"⚠ Session video not found: {video_name}")

                except Exception as e:
                    print(f"⚠ Error reading session file: {e}")

            # Alternative: Find videos by timestamp in filename
            if not video_files:
                print(f"Looking for videos with timestamp {session_timestamp} in filename...")
                pattern = f"video_{session_timestamp}_*.mp4"
                video_files = sorted(list(generated_ads_dir.glob(pattern)))

                if video_files:
                    print(f"Found {len(video_files)} videos matching timestamp pattern")

        if not video_files:
            print("⚠ No videos found for the specified session")
            print("Falling back to merging all videos in directory")
            session_only = False

    if not session_only or not video_files:
        # Fallback: Find all video files
        print("Merging all videos in directory")
        video_files = sorted(list(generated_ads_dir.glob('video_*.mp4')))

    if not video_files:
        print(f"✗ No video files found in {generated_ads_dir}")
        return False

    # Sort videos for consistent order
    video_files.sort()

    merge_type = f"session {session_timestamp}" if session_only and session_timestamp else "all available"
    print(f"\nFound {len(video_files)} videos to merge ({merge_type}):")
    for video in video_files:
        video_info = get_video_info(video)
        if video_info:
            print(f"  • {video.name} ({video_info['width']}x{video_info['height']}, {video_info['duration']:.1f}s)")
        else:
            print(f"  • {video.name}")

    # Create output filename
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    if session_timestamp and session_only:
        output_filename = f"merged_{session_timestamp}.mp4"
    else:
        output_filename = f"merged_{timestamp}.mp4"
    output_path = generated_ads_dir / output_filename

    # Merge videos
    if transition:
        success = merge_videos_with_transitions(video_files, output_path, transition_duration)
    else:
        success = merge_videos_concat(video_files, output_path)

    if success:
        output_info = get_video_info(output_path)
        if output_info:
            print(f"\n📹 Merged video details:")
            print(f"  Resolution: {output_info['width']}x{output_info['height']}")
            print(f"  Duration: {output_info['duration']:.1f} seconds")
            print(f"  Location: {output_path}")

        # Create merge report
        report = {
            "timestamp": timestamp,
            "session_timestamp": session_timestamp if session_only else None,
            "source_videos": [str(v.name) for v in video_files],
            "output_video": output_filename,
            "transition": transition,
            "transition_duration": transition_duration if transition else 0,
            "total_videos": len(video_files),
            "merge_type": merge_type
        }

        report_file = generated_ads_dir / f"merge_report_{timestamp}.json"
        with open(report_file, 'w') as f:
            json.dump(report, f, indent=2)

        print(f"  Report: {report_file}")

    return success

def main():
    parser = argparse.ArgumentParser(description='Merge generated video ads into a single video')
    parser.add_argument('folder', type=str,
                       help='Path to the folder containing generated_ads')
    parser.add_argument('--transition', action='store_true',
                       help='Add fade transitions between videos')
    parser.add_argument('--transition-duration', type=float, default=0.5,
                       help='Duration of transitions in seconds (default: 0.5)')
    parser.add_argument('--all-videos', action='store_true',
                       help='Merge all videos in directory instead of just current session')
    parser.add_argument('--timestamp', type=str,
                       help='Specific session timestamp to merge (format: YYYYMMDD_HHMMSS)')
    parser.add_argument('--videos', nargs='+',
                       help='Specific video files to merge (optional)')

    args = parser.parse_args()

    # Check ffmpeg installation
    if not check_ffmpeg():
        print("\n✗ Please install ffmpeg to merge videos")
        sys.exit(1)

    # Handle specific video files if provided
    if args.videos:
        video_files = []
        for video_path in args.videos:
            video_file = Path(video_path)
            if video_file.exists():
                video_files.append(video_file)
            else:
                print(f"✗ Video file not found: {video_path}")

        if not video_files:
            print("✗ No valid video files provided")
            sys.exit(1)

        # Create output path
        output_dir = video_files[0].parent
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_path = output_dir / f"merged_{timestamp}.mp4"

        # Merge videos
        if args.transition:
            success = merge_videos_with_transitions(video_files, output_path, args.transition_duration)
        else:
            success = merge_videos_concat(video_files, output_path)

        sys.exit(0 if success else 1)

    # Normal operation: merge videos from folder
    session_only = not args.all_videos
    success = merge_generated_videos(
        args.folder,
        transition=args.transition,
        transition_duration=args.transition_duration,
        session_only=session_only,
        use_timestamp=args.timestamp
    )

    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()