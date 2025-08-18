# main.py
"""
Animal Rescue Management System

This application processes animal rescue data from Google Drive and Instagram,
analyzes images using OpenAI's vision capabilities, and stores the data in Google Sheets.
"""
import os
import logging
import pandas as pd
import json
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
        self.worksheet_animal = self.sheet_service.get_worksheet("ANIMAL")
        self.worksheet_gastos = self.sheet_service.get_worksheet("GASTOS") 
        self.worksheet_eventos = self.sheet_service.get_worksheet("EVENTO") 
        self.worksheet_interaccion = self.sheet_service.get_worksheet("INTERACCION")
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
                            self.sheet_service.insert_sheet_from_dict(result, worksheet)
                    else:
                        self.sheet_service.insert_sheet_from_dict(analysis_results, worksheet)
                    
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
            oldest_date = self.sheet_service.get_oldest_date(self.worksheet_animal)
            oldest_id = self.sheet_service.get_oldest_id(self.worksheet_animal)
            # Fetch posts from Instagram API
            posts = self.instagram_api.get_recent_posts(oldest_date)
            logger.info(f"Retrieved {len(posts)} posts from Instagram")
            posts = self.filtrarPostNuevos(posts)
            
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
                    media_url = post.get("thumbnail_url") or post.get("media_url")
                    #if "adoptado" in caption or "actualizacion" in caption:
                    resp= self.image_analyzer.analyze_caption_post(caption, formatted_date )
                    print("resp" , resp)
                    image_bytes = []
                    # resp puede ser '0' o '["nombres",[[u,e,d],...]]'
                    if resp != 0 or resp != "0":
                        data = json.loads(resp) 
                        print(data)
                        print(len(data))
                        names = data[0].split(",") 
                        cant_names = len(names) 
                        print ("nombres. ", names)
                        for i, name in enumerate(names): 
                            id = self.sheet_service.get_id(name, self.worksheet_animal) 
                            print(name)
                            if id is None:
                                # Download and analyze image 
                                print("id none")
                                if cant_names == 1 or 'children' not in post :
                                    print("one animal")
                                    image_bytes = self.instagram_api.download_media(media_url)
                                else :
                                    for i , children in enumerate(post.get("children").get("data")[:cant_names-1]) : 
                                        #media_url =  children.get("thumbnail_url")  if "thumbnail_url" in children else  children.get("media_url")
                                        print("children")
                                        media_url = children.get("thumbnail_url", children.get("media_url"))

                                        image_bytes.append( self.instagram_api.download_media(media_url) )
        
                                # Analyze animal image
                                print ("data0: ", data[0])
                                results = self.image_analyzer.analyze_animal_image(image_bytes, formatted_date, caption,  data[0])
                                for i, result in enumerate(results):
                                    oldest_id = oldest_id +1 
                                    nuevo_registro = self.armar_datos_a_insertar(oldest_id,result) 
                                    self.sheet_service.insert_sheet_from_dict(nuevo_registro, self.worksheet_animal) 
                                    media_url= self.getmediaurl(post ,i,media_url)
                                    post_id_children = self.getchildrenid(post ,i,post_id) 
                                    nuevo_registro= self.armar_post_a_insertar(post_id_children,oldest_id,media_url,permalink, formatted_date)
                                    self.sheet_service.insert_sheet_from_dict(nuevo_registro, self.worksheet_interaccion)
                            else :
                                nuevo_registro= self.armar_post_a_insertar(post_id,id,media_url,permalink, formatted_date)
                                self.sheet_service.insert_sheet_from_dict(nuevo_registro, self.worksheet_interaccion)

                            for evento in data[1] :
                                print("evento" , evento)
                                id = id or (oldest_id - cant_names + 1) 
                                nuevo_evento = self.armar_estado_a_insertar(evento,id, formatted_date) 
                                self.sheet_service.insert_sheet_from_dict(nuevo_evento, self.worksheet_eventos)

                    logger.info(f"Successfully processed Instagram post {post_id}")
                    
                except Exception as e:
                    logger.error(f"Error processing Instagram post {post_id}: {str(e)}")
            
        except Exception as e:
            logger.error(f"Failed to process Instagram posts: {str(e)}")
    def getmediaurl(self,post,i,media_url):
        if 'children' in post :
            return post.get("children").get("data")[i].get("thumbnail_url") or post.get("children").get("data")[i].get("media_url") 
        else :
            return media_url
    def getchildrenid(self,post,i,post_id):
        if 'children' in post :
            return post.get("children").get("data")[i].get("id")
        else :
            return post_id

    def filtrarPostNuevos(self, post_list):
        # traer existentes y pasarlos a set para lookup O(1)
        df = pd.DataFrame(self.worksheet_interaccion.get_all_records())
        existentes = set(df['contenido'].dropna().astype(str)) if (not df.empty and 'contenido' in df.columns) else set()

        # filtrar por permalink
        post_nuevos = [p for p in post_list if str(p.get('permalink', '')) not in existentes]

        # ordenar por timestamp (más nuevos primero)
        post_nuevos.sort(
            key=lambda x: datetime.strptime(x['timestamp'], "%Y-%m-%dT%H:%M:%S%z"),
            reverse=False        )
        
        return post_nuevos

    def armar_datos_a_insertar(self,new_id,result):
        record_data ={ 
                    "id":new_id,
                    "nombre":result["Nombre"],
                    "fecha":result["Fecha"],
                    "tipo_animal":result["Tipo Animal"],
                    "ubicacion":result["Ubicacion"],
                    "edad":result["Edad"],
                    "color_de_pelo":result["Color de pelo"],
                    "condicion_de_salud_inicial":result["Condición de Salud Inicial"],
                    "activo": "TRUE",
                    "fecha_actualizacion":result["Fecha"]
                }
        return record_data
    
    def armar_estado_a_insertar(self,evento,id, formated_date):

        evento_nuevo= {                      "animal_id":id,
                                        "ubicacion_id" : evento[0],
                                        "estado_id":evento[1],                                        
                                        "persona_id":evento[3],
                                        "tipo_relacion_id":evento[4],
                                        "fecha":evento[2] or formated_date
                                        }
        return evento_nuevo
    
    def armar_post_a_insertar(self,post_id_children,nuevo_id,media_url,permalink,formatted_date):

        record_post={
            "animal_id":nuevo_id,
            "fecha": formatted_date,
            "post_id": post_id_children,
            "contenido":permalink,
            "media_url": media_url
        }

        return record_post

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
    
    def process_instagram_histories(self):
        pass
    

    def run(self):
        """Run the full application workflow."""
        logger.info("Starting Animal Rescue Manager")
        
        # Process images from Google Drive
        #self.process_drive_images(self.folder_recibos, self.worksheet_gastos, "recibo")
        #self.process_drive_images(self.folder_mascotas, self.worksheet_animal, "mascota")
        
        # Process Instagram posts
        self.process_instagram_posts()
        # Process Instagram histories
        self.process_instagram_histories()
        # Process financial transactions
        #self.process_transactions()
        
        logger.info("Processing complete")


if __name__ == "__main__":
    try:
        app = AnimalRescueManager()
        app.run()
        # Uncomment to run the voice assistant mode
        # app.run_voice_assistant()
    except Exception as e:
        logger.critical(f"Application failed: {str(e)}")