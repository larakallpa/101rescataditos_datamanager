# src/services/image_analysis.py
"""
Image Analysis Service

This module handles animal and receipt image analysis using OpenAI's vision capabilities.
"""
import json
import base64
import logging
from typing import Dict, List, Union, Optional
from openai import OpenAI
import os

logger = logging.getLogger(__name__)

class ImageAnalyzer:
    """Handles image analysis using AI vision models."""
    
    def __init__(self):
        """Initialize the image analyzer with the OpenAI client."""
        self.client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
        logger.info("Image analyzer initialized")
        
    def _clean_json_response(self, response: str) -> str:
        """Clean JSON response by removing markdown code blocks.
        
        Args:
            response: The raw response from the AI model
            
        Returns:
            A clean JSON string
        """
        if response.startswith("```json"):
            response = response.replace("```json", "").strip()
        if response.endswith("```"):
            response = response[:-3].strip()
        return response
    
    def analyze_animal_image(self,image_bytes: bytes,date_time: str,caption: Optional[str],image_id: str,url: str) -> List[Dict]:
        """Analyze an animal image using OpenAI's vision model.
        
        Args:
            image_bytes: The raw image data
            date_time: Formatted timestamp
            caption: Optional image caption or description
            image_id: ID of the image (from Drive or Instagram)
            url: URL to access the image
            
        Returns:
            List of dictionaries containing analyzed animal data
        """
        logger.info(f"Analyzing animal image with ID: {image_id}")
        
        try:
            # Convert image to base64
            base64_image = base64.b64encode(image_bytes).decode()
            caption_text = caption or ""
            
            # Define system prompt with analysis requirements
            system_prompt = self._get_animal_prompt_template()
            
            # Call OpenAI API for image analysis
            response = self.client.chat.completions.create(
                model="gpt-4o",
                messages=[
                    {"role": "system", "content": system_prompt},
                    {
                        "role": "user",
                        "content": [
                            {
                                "type": "text",
                                "text": (
                                    f"Descripción del post:\n{caption_text}\n\n"
                                    "Analiza esta imagen junto al texto y completa los datos requeridos:"
                                )
                            },
                            {
                                "type": "image_url",
                                "image_url": {
                                    "url": f"data:image/jpeg;base64,{base64_image}"
                                }
                            }
                        ]
                    }
                ],
                max_tokens=600
            )
            
            result = response.choices[0].message.content.strip()
            
            # Handle "IGNORAR" response (image should be ignored)
            if result.upper() == "IGNORAR":
                logger.info(f"Image {image_id} marked for ignoring (not an animal)")
                return []
            
            # Clean and parse JSON response
            cleaned_response = self._clean_json_response(result)
            analysis = json.loads(cleaned_response)
            
            # Process color information
            color_info = analysis.get("color_pelo", "No determinado")
            if isinstance(color_info, list):
                color_string = json.dumps(color_info, ensure_ascii=False)
            else:
                color_string = str(color_info)
            
            # Process names (multiple animals may be in one image)
            names_raw = analysis.get("nombre", "No detectado")
            if names_raw != "No detectado":
                names = [name.strip() for name in names_raw.split(",")]
            else:
                names = ["No detectado"]
            
            # Create records for each animal detected
            records = []
            for name in names:
                record = {
                    "Nombre": name,
                    "Fecha": date_time,
                    "Tipo Animal": analysis.get("tipo_animal", "No determinado"),
                    "Ubicacion": analysis.get("Ubicacion", "No determinado"),
                    "Color de pelo": color_string,
                    "Edad": analysis.get("Edad", "No determinado"),
                    "Estado Actual": analysis.get("Estado Actual", "No determinado"),
                    "Condición de Salud Inicial": analysis.get("Condición de Salud Inicial", "No determinado"),
                    "Adoptante": analysis.get("adoptante", ""),
                    "id_post": image_id,
                    "url_instagram": url,
                    "url_drive": ""
                }
                records.append(record)
            
            logger.info(f"Successfully analyzed image {image_id}, found {len(records)} animals")
            return records
            
        except Exception as e:
            logger.error(f"Error analyzing animal image {image_id}: {str(e)}")
            return []
    
    def analyze_receipt_image(self,image_bytes: bytes,date_time: str,url: str,name: str,image_id: str ) -> Dict:
        """Analyze a receipt image using OpenAI's vision model.
        
        Args:
            image_bytes: The raw image data
            date_time: Formatted timestamp 
            url: URL to access the image
            name: Name of the image file
            image_id: ID of the image
            
        Returns:
            Dictionary containing analyzed receipt data
        """
        logger.info(f"Analyzing receipt image: {name}")
        
        try:
            # Convert image to base64
            base64_image = base64.b64encode(image_bytes).decode()
            
            # Define system prompt with analysis requirements
            system_prompt = self._get_receipt_prompt_template()
            
            # Call OpenAI API for image analysis
            response = self.client.chat.completions.create(
                model="gpt-4o",
                messages=[
                    {"role": "system", "content": system_prompt},
                    {
                        "role": "user",
                        "content": [
                            {
                                "type": "text",
                                "text": (
                                    "Analiza esta imagen de un recibo y completa los siguientes datos para cargar en un Excel: "
                                    "Fecha, Proveedor, Tipo de Gasto, Mascota, Responsable, Detalle, Monto, Forma de Pago, "
                                    "Observaciones."
                                )
                            },
                            {
                                "type": "image_url",
                                "image_url": {
                                    "url": f"data:image/jpeg;base64,{base64_image}"
                                }
                            }
                        ]
                    }
                ],
                max_tokens=700
            )
            
            result = response.choices[0].message.content
            
            # Clean and parse JSON response
            cleaned_response = self._clean_json_response(result)
            receipt_data = json.loads(cleaned_response)
            
            # Add additional metadata
            receipt_data["Foto"] = url
            receipt_data["id_foto"] = image_id
            
            logger.info(f"Successfully analyzed receipt {name}")
            return receipt_data
            
        except Exception as e:
            logger.error(f"Error analyzing receipt image {name}: {str(e)}")
            return {}
    
    def _get_animal_prompt_template(self) -> str:
        """Return the system prompt template for animal image analysis."""
        json_example = (
            '{\n'
            '  "nombre": "extraído del texto si aparece",\n'
            '  "tipo_animal": "perro o gato",\n'
            '  "color_pelo": [\n'
            '    { "color": "color1", "porcentaje": porcentaje1 },\n'
            '    { "color": "color2", "porcentaje": porcentaje2 }\n'
            '  ],\n'
            '  "Edad": "estimar edad en años o meses sin adicionales",\n'
            '  "Condición de Salud Inicial": "describir cómo fue recibido",\n'
            '  "Estado Actual": "En adopción / En tratamiento / Adoptado / Perdido / Fallecido",\n'
            '  "Ubicacion": "Informar donde fue encontrado si aparece en la descripción de la foto"\n'
            '  "adoptante": "@usuario" o "No especificado"\n'
            '}'
        )
        
        return (
            "Eres un asistente que analiza imágenes de mascotas y sus descripciones en redes sociales. "
            "Tu tarea es generar un JSON válido con la siguiente estructura:\n\n"
            + json_example +
            "\n\n"
            "Antes de generar el JSON, evaluá si el contenido se refiere claramente a una o más mascotas reales."
            "Si no hay una mascota visible o mencionada en el texto (por ejemplo si es una imagen informativa, un sorteo, cartel, o gráfico general)," 
            "no generes ningún JSON. Simplemente respondé con la palabra: IGNORAR."
            "Debes basarte tanto en la imagen como en el texto que la acompaña. Seguí estas instrucciones:\n"
            "1. Extraé el nombre del animal si aparece. Puede estar en frases como:\n"
            "   - 'Soy ___'\n"
            "   - 'Se llama ___'\n"
            "   - 'Info sobre ___'\n"
            "   - 'Este es ___'\n"
            "2. Si aparecen más de un nombre **de animales distintos**, separalos por comas, sin espacios. Ejemplo: 'Lia,Tomy'.\n"
            "3. **No se deben considerar como nombres distintos** a los diminutivos, formas cariñosas o abreviaciones del mismo nombre. "
            "Ejemplo: 'Uva' y 'Uvita' se consideran el mismo animal. Usá el nombre más común o simple (por ejemplo, 'Uva').\n"
            "4. Usá cualquier mención a enfermedades, tratamientos o condiciones para completar:\n"
            "   - 'Condición de Salud Inicial'\n"
            "   - 'Estado Actual'\n"
            "5. Estimá la edad del animal si es posible (años o meses, sin agregar 'aproximadamente', 'más o menos', etc.).\n"
            "6. Si se menciona un lugar o barrio donde fue encontrado o rescatado, usalo en 'Ubicacion'.\n"
            "7. Si podés identificar colores predominantes del pelaje, agregalos con un porcentaje aproximado (máximo 2 colores).\n\n"
            "⚠ IMPORTANTE: No incluyas el bloque de código markdown ni ningún otro tipo de formato. Respondé solamente con el contenido entre { y }"
            "La respuesta debe empezar con { y terminar con }."
            "8. El campo 'Edad' debe incluir siempre la unidad de tiempo ('años' o 'meses'). Nunca debe aparecer como un número solo.- Ejemplo válido: '2 años', '6 meses'- Ejemplo inválido: '2' \n"
            "9. Si el texto menciona a la persona que adoptó (por ejemplo con una cuenta como '@usuario'), incluila en un campo llamado 'adoptante'. Si no se menciona, dejalo como ''"
        )
    
    def _get_receipt_prompt_template(self) -> str:
        """Return the system prompt template for receipt image analysis."""
        json_example = (
            '{\n'
            '  "Fecha": "25/01/2024 15:02:24",\n'
            '  "Proveedor": "Centro Veterinario Linares",\n'
            '  "Tipo de Gasto": "Puede ser Veterinaria  Alimentos en el detalle iria medicacion farmacio u otros",\n'
            '  "Mascota": "Nombre de la mascota si figura",\n'
            '  "Responsable": "Nombre del cliente si figura en el ticket",\n'
            '  "Detalle": "APLICACION INTRAMUS. /S. CUTANEA.",\n'
            '  "Monto": 3000.00,\n'
            '  "Forma de Pago": "MERCADOPAGO",\n'
            '  "Observaciones": ""\n'
            '}'
        )
        
        return (
            "Eres un asistente que analiza imágenes de recibos o facturas para cargar datos en un Excel. "
            "Devuelve SOLO un objeto JSON con los siguientes campos: \n"
            + json_example +
            "\nSi algún campo no está presente o no se puede deducir de la imagen, usa ' '.\n"
            "IMPORTANTE: Devuelve ÚNICAMENTE el objeto JSON sin marcadores de código, sin texto explicativo adicional. "
            "El JSON debe comenzar con el carácter { y terminar con }"
        )