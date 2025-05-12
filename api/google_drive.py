# src/api/google_drive.py
"""
Google Drive API Module

This module handles interactions with Google Drive API for file management.
"""
import os
import logging
import requests
from typing import List, Dict, Optional, Any
from googleapiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials

logger = logging.getLogger(__name__)

class DriveAPI:
    """Handles operations with Google Drive API."""
    
    def __init__(self):
        """Initialize the Drive API client with credentials."""
        try:
            # Set up credentials
            scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
            self.creds = ServiceAccountCredentials.from_json_keyfile_name(
                "credenciales.json", 
                scopes=scope
            )
            
            # Initialize Drive API service
            self.service = build('drive', 'v3', credentials=self.creds)
            logger.info("Google Drive API initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize Google Drive API: {str(e)}")
            raise
    
    def list_files(self, folder_id: str) -> List[Dict[str, Any]]:
        """List image files in a specific Google Drive folder.
        
        Args:
            folder_id: ID of the Google Drive folder to list files from
            
        Returns:
            List of file metadata dictionaries
        """
        try:
            # Create query to find images in the specified folder
            query = f"'{folder_id}' in parents and mimeType contains 'image/' and trashed = false"
            
            # Execute the query
            results = self.service.files().list(
                q=query,
                fields="files(id, name, createdTime)"
            ).execute()
            
            files = results.get('files', [])
            logger.info(f"Found {len(files)} files in folder {folder_id}")
            return files
            
        except Exception as e:
            logger.error(f"Error listing files in folder {folder_id}: {str(e)}")
            return []
    
    def download_file(self, file_id: str) -> Optional[bytes]:
        """Download a file from Google Drive by its ID.
        
        Args:
            file_id: The Google Drive file ID
            
        Returns:
            File content as bytes or None if download failed
        """
        try:
            # Create authorized request with OAuth token
            headers = {"Authorization": f"Bearer {self.creds.access_token}"}
            url = f"https://www.googleapis.com/drive/v3/files/{file_id}?alt=media"
            
            # Execute request
            response = requests.get(url, headers=headers)
            
            if response.status_code == 200:
                logger.info(f"Successfully downloaded file {file_id}")
                return response.content
            else:
                logger.error(f"Error downloading file {file_id}: HTTP {response.status_code}")
                return None
                
        except Exception as e:
            logger.error(f"Error downloading file {file_id}: {str(e)}")
            return None
    
    def move_file(self, file_id: str, destination_folder_id: str) -> bool:
        """Move a file to a different folder in Google Drive.
        
        Args:
            file_id: ID of the file to move
            destination_folder_id: ID of the destination folder
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Get the current parents of the file
            file = self.service.files().get(
                fileId=file_id, 
                fields='parents'
            ).execute()
            
            previous_parents = ",".join(file.get('parents', []))
            
            # Update the file's parent folder
            self.service.files().update(
                fileId=file_id,
                addParents=destination_folder_id,
                removeParents=previous_parents,
                fields='id, parents'
            ).execute()
            
            logger.info(f"Successfully moved file {file_id} to folder {destination_folder_id}")
            return True
            
        except Exception as e:
            logger.error(f"Error moving file {file_id} to folder {destination_folder_id}: {str(e)}")
            return False
