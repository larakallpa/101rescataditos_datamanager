# main.py
"""
Animal Rescue Management System

This application processes animal rescue data from Google Drive and Instagram,
analyzes images using OpenAI's vision capabilities, and stores the data in Google Sheets.
"""
import os
import logging
from datetime import datetime
from dotenv import load_dotenv

# Import services
from services.audio_service import AudioProcessor
from services.image_analysis import ImageAnalyzer
from services.sheet_service import SheetService
from services.transaction_service import TransactionProcessor
from api.google_drive import DriveAPI
from api.instagram import InstagramAPI

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("app.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class AnimalRescueManager:
    """Main application controller for the Animal Rescue Management System."""
    
    def __init__(self):
        """Initialize the application with required components and settings."""
        # Load environment variables
        load_dotenv()
        
        # Initialize key services
        self.sheet_service = SheetService()
        self.drive_api = DriveAPI()
        self.image_analyzer = ImageAnalyzer()
        self.instagram_api = InstagramAPI()
        self.audio_processor = AudioProcessor()
        self.transaction_processor = TransactionProcessor()
        
        # Load folder IDs from environment
        self.folder_mascotas = os.getenv("FOLDER_MASCOTAS")
        self.folder_recibos = os.getenv("FOLDER_RECIBOS")
        self.folder_ok_tickets = os.getenv("FOLDER_OK_TICKETS")
        self.folder_error_tickets = os.getenv("FOLDER_ERROR_TICKETS")
        
        # Access worksheets
        self.worksheet_datos = self.sheet_service.get_worksheet("Datos")
        self.worksheet_gastos = self.sheet_service.get_worksheet("Gastos")
        
        logger.info("Animal Rescue Manager initialized successfully")
    
    def process_drive_images(self, folder_id, worksheet, image_type="mascota"):
        """Process new images from Google Drive folder.
        
        Args:
            folder_id: The ID of the Google Drive folder
            worksheet: The Google Sheet worksheet to update
            image_type: Type of analysis to perform ('mascota' or 'recibo')
        """
        logger.info(f"Processing images from folder: {folder_id}")
        
        try:
            files = self.drive_api.list_files(folder_id)
            logger.info(f"Found {len(files)} files to process")
            
            for file_data in files:
                logger.info(f"Processing file: {file_data['name']}")
                
                try:
                    # Download and analyze image
                    file_id = file_data['id']
                    file_name = file_data['name']
                    creation_time = datetime.fromisoformat(file_data['createdTime'].replace('Z', '+00:00'))
                    formatted_date = creation_time.strftime('%Y-%m-%d %H:%M:%S')
                    drive_url = f"https://drive.google.com/uc?id={file_id}"
                    
                    image_bytes = self.drive_api.download_file(file_id)
                    if not image_bytes:
                        raise ValueError("Failed to download image")
                    
                    # Analyze image based on type
                    if image_type == "mascota":
                        analysis_results = self.image_analyzer.analyze_animal_image(
                            image_bytes, formatted_date, None, file_name, file_id
                        )
                    else:  # recibo
                        analysis_results = self.image_analyzer.analyze_receipt_image(
                            image_bytes, formatted_date, drive_url, file_name, file_id
                        )
                    
                    # Update spreadsheet
                    if isinstance(analysis_results, list):
                        for result in analysis_results:
                            self.sheet_service.update_sheet_from_dict(result, worksheet)
                    else:
                        self.sheet_service.update_sheet_from_dict(analysis_results, worksheet)
                    
                    # Move file to success folder
                    self.drive_api.move_file(file_id, self.folder_ok_tickets)
                    logger.info(f"Successfully processed {file_name}")
                    
                except Exception as e:
                    logger.error(f"Error processing file {file_data['name']}: {str(e)}")
                    self.drive_api.move_file(file_id, self.folder_error_tickets)
        
        except Exception as e:
            logger.error(f"Failed to process folder {folder_id}: {str(e)}")
    
    def process_instagram_posts(self):
        """Process recent Instagram posts and add them to the worksheet."""
        try:
            # Get the oldest date in our spreadsheet to know which posts to fetch
            oldest_date = self.sheet_service.get_oldest_date(self.worksheet_datos)
            
            # Fetch posts from Instagram API
            posts = self.instagram_api.get_recent_posts(oldest_date)
            logger.info(f"Retrieved {len(posts)} posts from Instagram")
            
            # Process each post
            for post in posts:
                try:
                    # Extract post data
                    caption = post.get("caption", "")
                    timestamp = datetime.strptime(post.get("timestamp"), "%Y-%m-%dT%H:%M:%S%z")
                    formatted_date = timestamp.strftime("%d/%m/%Y %H:%M:%S")
                    post_id = post.get("id")
                    permalink = post.get("permalink")
                    
                    # Get image URL based on media type
                    media_url = post.get("media_url")
                    if post.get("media_type") == "VIDEO" and post.get("thumbnail_url"):
                        media_url = post.get("thumbnail_url")
                    
                    if not media_url:
                        logger.warning(f"Post {post_id} has no image, skipping.")
                        continue
                    
                    # Download and analyze image
                    image_bytes = self.instagram_api.download_media(media_url)
                    if not image_bytes:
                        logger.warning(f"Could not download media for post {post_id}")
                        continue
                    
                    # Analyze animal image
                    results = self.image_analyzer.analyze_animal_image(image_bytes, formatted_date, caption, post_id, permalink)
                    
                    # Update spreadsheet with results
                    for result in results:
                        self.sheet_service.update_sheet_from_dict(result, self.worksheet_datos)
                    
                    logger.info(f"Successfully processed Instagram post {post_id}")
                    
                except Exception as e:
                    logger.error(f"Error processing Instagram post {post.get('id', 'unknown')}: {str(e)}")
            
        except Exception as e:
            logger.error(f"Failed to process Instagram posts: {str(e)}")
    
    def run_voice_assistant(self):
        """Run the voice command assistant for interactive usage."""
        logger.info("Starting voice assistant mode")
        
        try:
            self.audio_processor.run_assistant(self.sheet_service)
        except Exception as e:
            logger.error(f"Voice assistant error: {str(e)}")
    
    def process_transactions(self):
        """Process financial transactions from PDFs and Excel files."""
        logger.info("Starting transaction processing")
        self.transaction_processor.process()

    def run(self):
        """Run the full application workflow."""
        logger.info("Starting Animal Rescue Manager")
        
        # Process images from Google Drive
        self.process_drive_images(self.folder_recibos, self.worksheet_gastos, "recibo")
        self.process_drive_images(self.folder_mascotas, self.worksheet_datos, "mascota")
        
        # Process Instagram posts
        self.process_instagram_posts()
        # Process Instagram histories
        self.process_instagram_histories()
        # Process financial transactions
        self.process_transactions()
        
        logger.info("Processing complete")


if __name__ == "__main__":
    try:
        app = AnimalRescueManager()
        app.run()
        # Uncomment to run the voice assistant mode
        # app.run_voice_assistant()
    except Exception as e:
        logger.critical(f"Application failed: {str(e)}")