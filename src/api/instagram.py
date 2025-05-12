# src/api/instagram.py
"""
Instagram API Module

This module handles interactions with Instagram's Graph API to fetch posts and media.
"""
import os
import logging
import requests
from datetime import datetime
from typing import List, Dict, Any, Optional

logger = logging.getLogger(__name__)

class InstagramAPI:
    """Handles operations with the Instagram Graph API."""
    
    def __init__(self):
        """Initialize Instagram API client with access token and user ID."""
        self.access_token = os.getenv("FACEBOOK_ACCESS_TOKEN")
        self.user_id = os.getenv("IG_USER_ID")
        
        if not self.access_token or not self.user_id:
            logger.error("Instagram API credentials missing from environment variables")
        else:
            logger.info("Instagram API client initialized")
    
    def get_recent_posts(self, earliest_date: Optional[datetime] = None, limit: int = 10) -> List[Dict[str, Any]]:
        """Fetch recent Instagram posts.
        
        Args:
            earliest_date: Optional date to filter posts (only posts before this date)
            limit: Maximum number of posts to fetch initially
            
        Returns:
            List of post metadata dictionaries
        """
        if not self.access_token or not self.user_id:
            logger.error("Instagram API credentials not configured")
            return []
        
        try:
            # If no earliest date provided, use a default
            if earliest_date is None:
                earliest_date = datetime(2020, 1, 1, 0, 0, 0)
            
            logger.info(f"Fetching Instagram posts before {earliest_date}")
            
            # Initial API request URL and parameters
            url = f"https://graph.facebook.com/v22.0/{self.user_id}/media"
            params = {
                'fields': 'id,media_type,media_url,thumbnail_url,caption,timestamp,permalink',
                'access_token': self.access_token,
                'limit': limit
            }
            
            all_posts = []
            relevant_posts = []
            
            # Fetch posts with pagination
            while url:
                response = requests.get(url, params=params)
                
                if response.status_code != 200:
                    logger.error(f"Instagram API error: {response.status_code} - {response.text}")
                    break
                
                data = response.json()
                
                # Process this batch of posts
                posts_batch = data.get("data", [])
                all_posts.extend(posts_batch)
                
                # Filter posts by date
                for post in posts_batch:
                    post_time = datetime.strptime(post["timestamp"], "%Y-%m-%dT%H:%M:%S%z").replace(tzinfo=None)
                    if post_time > earliest_date:
                        relevant_posts.append(post)
                
                # Get next pagination URL if available
                url = data.get("paging", {}).get("next")
                params = {}  # Reset params as they're included in the next URL
            
            # Sort posts by date (newest first)
            relevant_posts.sort(
                key=lambda x: datetime.strptime(x["timestamp"], "%Y-%m-%dT%H:%M:%S%z").replace(tzinfo=None),
                reverse=True
            )
            
            logger.info(f"Retrieved {len(relevant_posts)} relevant posts from Instagram")
            return relevant_posts
            
        except Exception as e:
            logger.error(f"Error fetching Instagram posts: {str(e)}")
            return []
    
    def download_media(self, media_url: str) -> Optional[bytes]:
        """Download media from a URL.
        
        Args:
            media_url: URL of the media to download
            
        Returns:
            Media content as bytes or None if download failed
        """
        try:
            response = requests.get(media_url, timeout=10)
            
            if response.status_code == 200:
                return response.content
            else:
                logger.error(f"Failed to download media. Status code: {response.status_code}")
                return None
                
        except Exception as e:
            logger.error(f"Error downloading media from {media_url}: {str(e)}")
            return None
    
    def publish_post(self, image_path: str, caption: str) -> bool:
        """Publish a new post to Instagram (placeholder method).
        
        Note: This is a placeholder. Instagram Graph API has specific requirements
        for publishing that may require additional permissions and setup.
        
        Args:
            image_path: Path to the image file
            caption: Caption text for the post
            
        Returns:
            True if successful, False otherwise
        """
        # This would require Container approach with Facebook Graph API
        # which has several prerequisites and requirements
        logger.warning("Instagram post publishing not implemented")
        return False