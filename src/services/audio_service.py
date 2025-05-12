# src/services/audio_service.py
"""
Audio Service Module

This module handles voice commands processing using Whisper for transcription
and OpenAI for interpretation.
"""
import os
import json
import logging
import speech_recognition as sr
from openai import OpenAI
from typing import Dict, Optional, Any

logger = logging.getLogger(__name__)

class AudioProcessor:
    """Handles voice command processing for animal management system."""
    
    def __init__(self):
        """Initialize audio processor with OpenAI client and recognizer."""
        self.client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
        self.recognizer = sr.Recognizer()
        logger.info("Audio processor initialized")
    
    def listen_for_command(self) -> Optional[str]:
        """Record audio from microphone and transcribe it using Whisper.
        
        Returns:
            Transcribed text or None if transcription failed
        """
        temp_audio_path = "temp_audio.wav"
        
        try:
            logger.info("Listening for voice command...")
            with sr.Microphone() as source:
                print("Habla ahora...")
                self.recognizer.adjust_for_ambient_noise(source)
                audio = self.recognizer.listen(source)
            
            # Save audio to temporary file
            with open(temp_audio_path, "wb") as f:
                f.write(audio.get_wav_data())
            
            # Transcribe with Whisper API
            with open(temp_audio_path, "rb") as audio_file:
                transcript = self.client.audio.transcriptions.create(
                    model="whisper-1",
                    file=audio_file,
                    language="es"
                )
            
            transcribed_text = transcript.text
            logger.info(f"Transcribed: {transcribed_text}")
            return transcribed_text
            
        except Exception as e:
            logger.error(f"Error processing audio: {str(e)}")
            return None
            
        finally:
            # Clean up temporary file
            if os.path.exists(temp_audio_path):
                os.remove(temp_audio_path)
    
    def interpret_command(self, command_text: str) -> Optional[Dict[str, Any]]:
        """Interpret voice command text using OpenAI.
        
        Args:
            command_text: Transcribed voice command
            
        Returns:
            Dictionary with parsed command data or None if parsing failed
        """
        if not command_text:
            return None
            
        try:
            logger.info(f"Interpreting command: {command_text}")
            
            # Create prompt for command interpretation
            prompt = f"""
Eres un asistente inteligente que interpreta comandos por voz para editar un registro de animales rescatados en Google Sheets. 
La hoja tiene las siguientes columnas:  "Nombre", "Tipo Animal", "Lugar donde fue encontrado", "Edad", "Color de pelo", 
"Condición de Salud Inicial", "Estado Actual", "Fecha de Adopcion", "Adoptante".

El campo "Nombre" se usará para identificar la fila a actualizar.

Algunos ejemplos de comandos que podrías recibir:
- "Actualiza el animal con Nombre Juan, cambia su estado actual a adoptado y la fecha de adopción a hoy"
- "Registra que el perro con Nombre Pepe fue adoptado por Juan Pérez hoy"
- "Cambia la condición de salud del gato con nombre arenita a recuperado"
- "Para Pablo, actualiza el lugar donde fue encontrado a Parque Central"

Dado el siguiente comando de voz: "{command_text}"

Extrae los datos de la siguiente forma:
- "Nombre": El identificador que se utilizará para buscar la fila a actualizar.

Devuelve solo un JSON con el siguiente formato:
{{
  "Nombre": "Juan",
  "Fecha": "2025-04-05",
  "Hora": "20:00:00",
  "Tipo Animal": "Gato",
  "Edad":"12"
}}

Para comandos que parezcan crear un nuevo registro, crear el json con los campos fecha y hora actual e incluye todos los campos disponibles.
Si el comando no es válido o no contiene suficiente información, devuelve: {{"error": "Comando incompleto"}}
"""

            # Call OpenAI API for interpretation
            response = self.client.chat.completions.create(
                model="gpt-3.5-turbo-0125",
                response_format={"type": "json_object"},
                messages=[
                    {"role": "system", "content": "Eres un asistente especializado en gestión de refugios animales."},
                    {"role": "user", "content": prompt}
                ]
            )
            
            response_text = response.choices[0].message.content 
            
            # Parse and validate the JSON response
            command_data = json.loads(response_text)
            
            if "error" in command_data:
                logger.warning(f"Command interpretation returned error: {command_data['error']}")
                return None
                
            logger.info(f"Command interpreted: {command_data}")
            return command_data
            
        except json.JSONDecodeError:
            logger.error(f"Failed to parse JSON from response: {response_text}")
            return None
        except Exception as e:
            logger.error(f"Error interpreting command: {str(e)}")
            return None
    
    def provide_feedback(self, message: str):
        """Provide feedback to the user.
        
        Args:
            message: The feedback message to provide
        """
        # For now, just print the message
        # This could be expanded to use text-to-speech
        print(f"🔊 {message}")
        logger.info(f"Feedback provided: {message}")
    
    def run_assistant(self, sheet_service):
        """Run the voice assistant in a continuous loop.
        
        Args:
            sheet_service: SheetService instance to update spreadsheets
        """
        logger.info("Starting voice assistant")
        print("🎙️ Asistente de Registro Animal activado. Di 'salir' para terminar.")
        
        worksheet = sheet_service.get_worksheet("Datos")
        headers = sheet_service.get_headers(worksheet)
        print(f"📋 Columnas disponibles: {', '.join(headers)}")
        
        while True:
            # Listen for command
            command_text = self.listen_for_command()
            
            if not command_text:
                self.provide_feedback("No pude entender, por favor intente de nuevo.")
                continue
                
            # Check for exit command
            if command_text.lower() in ["salir", "terminar", "finalizar"]:
                self.provide_feedback("¡Hasta luego!")
                break
                
            # Interpret and execute command
            command_data = self.interpret_command(command_text)
            
            if command_data:
                try:
                    # Update sheet with command data
                    sheet_service.update_sheet_from_dict(command_data, worksheet)
                    self.provide_feedback("Actualización completada.")
                except Exception as e:
                    logger.error(f"Error executing command: {str(e)}")
                    self.provide_feedback("No pude realizar la acción solicitada.")
            else:
                self.provide_feedback("No pude interpretar el comando.")