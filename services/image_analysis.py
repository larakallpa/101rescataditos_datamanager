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
    
    def analyze_animal_image(self,image_bytes,date_time: str,caption: Optional[str], names) -> List[Dict]:
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
        try: 
            caption_text = caption or ""
            
            # Define system prompt with analysis requirements
            system_prompt = self._get_animal_prompt_template() + " " + names
     
            content = []
            
            # Agregar texto inicial
            content.append({
                "type": "text",
                "text": (
                    f"Descripción del post:\n{caption_text}\n\n"
                    "Analiza esta(s) imagen(es) junto al texto:"
                )
            })
            
            # Manejar una o múltiples imágenes
            if isinstance(image_bytes, list):
                # MÚLTIPLES IMÁGENES
                for i, img_bytes in enumerate(image_bytes):
                    base64_img = base64.b64encode(img_bytes).decode()
                    content.append({
                        "type": "text", 
                        "text": f"\n--- Imagen {i+1} ---"
                    })
                    content.append({
                        "type": "image_url",
                        "image_url": {"url": f"data:image/jpeg;base64,{base64_img}"}
                    })
            else:
                # UNA SOLA IMAGEN (tu código original)
                base64_image = base64.b64encode(image_bytes).decode()
                content.append({
                    "type": "image_url",
                    "image_url": {"url": f"data:image/jpeg;base64,{base64_image}"}
                })
            
            # Resto igual, pero usar 'content' en lugar del objeto hardcodeado
            response = self.client.chat.completions.create(
                model="gpt-4o",
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": content}  # ← USAR LA LISTA DINÁMICA
                ],
                max_tokens=800  # Aumentar para múltiples imágenes
            )
            result = response.choices[0].message.content.strip() 
            print("resultado", result)

            # Handle "IGNORAR" response (image should be ignored)
            if result.upper() == "IGNORAR":
                logger.info(f"Image {names} marked for ignoring (not an animal)")
                return []

            # Clean and parse JSON response - ahora esperamos un array
            cleaned_response = self._clean_json_response(result)

            try:
                analysis_array = json.loads(cleaned_response)
                
                # Si por alguna razón devuelve un objeto en lugar de array, convertir
                if isinstance(analysis_array, dict):
                    analysis_array = [analysis_array]
                elif not isinstance(analysis_array, list):
                    logger.error(f"Unexpected response format: {type(analysis_array)}")
                    return []
                    
            except json.JSONDecodeError as e:
                logger.error(f"Error parsing JSON response: {e}")
                logger.error(f"Raw response: {result}")
                return []

            print("avanzando")

            # Process each animal in the array
            records = []
            for animal_data in analysis_array:
                # Process color information
                color_info = animal_data.get("color_pelo", "No determinado")
                if isinstance(color_info, list):
                    color_string = json.dumps(color_info, ensure_ascii=False)
                else:
                    color_string = str(color_info)
                
                record = {
                    "Nombre": animal_data.get("Nombre", "Sin nombre"),
                    "Fecha": date_time,
                    "Tipo Animal": animal_data.get("tipo_animal", "No determinado"),
                    "Ubicacion": animal_data.get("Ubicacion", "No determinado"),
                    "Color de pelo": color_string,
                    "Edad": animal_data.get("Edad", "No determinado"), 
                    "Condición de Salud Inicial": animal_data.get("Condición de Salud Inicial", "No determinado")
                }
                records.append(record)

            logger.info(f"Successfully analyzed image {names}, found {len(records)} records")
            print(records)
            return records
        except Exception as e:
            logger.error(f"Error analyzing animal image {names}: {str(e)}")
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
    
    def analyze_caption_post(self,caption: str, timestamp) -> list[str]:
        logging.info("Analizando caption")
        prompt = self._get_caption_prompt_template()

        # Call OpenAI API for image analysis
        response = self.client.chat.completions.create(
                model="gpt-4o",
                messages=[
                    {"role": "system", "content": prompt + " FECHA_PUBLICACION: " +timestamp},
                    {
                        "role": "user",
                        "content": [
                            {
                                "type": "text",
                                "text": (
                                    f"Descripción del post:\n{caption}\n\n" 
                                )
                            }
                        ]
                    }
                ],
                max_tokens=700
            ) 
        result = response.choices[0].message.content
        print("result chatgpt ",result)
        if result !=0:
            resultj =json.loads(result) 
            names= resultj[0].split(",") 
            print("result json ",resultj)
            logger.info(f"Successfully ,found {len(names)} animals")
        return result

 
    
    def _get_animal_prompt_template(self) -> str:
        """Return the system prompt template for animal image analysis."""
        json_example = (
            '[\n'
            '  {\n'
            '    "Nombre": "nombre_individual_del_animal",\n'
            '    "tipo_animal": "perro o gato",\n'
            '    "color_pelo": [\n'
            '      { "color": "color1", "porcentaje": 70 },\n'
            '      { "color": "color2", "porcentaje": 30 }\n'
            '    ],\n'
            '    "Edad": "2 años",\n'
            '    "Condición de Salud Inicial": "describir cómo fue recibido",\n'
            '    "Ubicacion": "lugar donde fue encontrado"\n'
            '  },\n'
            '  {\n'
            '    "Nombre": "otro_animal_si_hay_varios",\n'
            '    "tipo_animal": "perro o gato",\n'
            '    "color_pelo": [\n'
            '      { "color": "negro", "porcentaje": 100 }\n'
            '    ],\n'
            '    "Edad": "6 meses",\n'
            '    "Condición de Salud Inicial": "sano",\n'
            '    "Ubicacion": "CABA"\n'
            '  }\n'
            ']\n'
        )
        
        return (
            "Eres un asistente que analiza imágenes de mascotas y sus descripciones en redes sociales. "
            "Tu tarea es generar un JSON ARRAY válido con la siguiente estructura:\n\n"
            + json_example +
            "\n\n"
            "REGLAS IMPORTANTES:\n"
            "1. Si hay múltiples animales mencionados, crea UN OBJETO JSON SEPARADO para cada animal dentro del array\n"
            "2. Cada animal debe tener su propio objeto con su nombre individual (NUNCA concatenes nombres)\n"
            "3. Si solo hay un animal, devuelve un array con un solo objeto\n"
            "4. La respuesta debe empezar con [ y terminar con ]\n"
            "5. Si no hay mascotas visibles o mencionadas (imagen informativa, sorteo, cartel), respondé: IGNORAR\n\n"
            
            "INSTRUCCIONES DE ANÁLISIS:\n"
            "• Basate tanto en la imagen como en el texto que la acompaña\n"
            "• Estimá la edad del animal si es posible (siempre incluir 'años' o 'meses')\n"
            "• Si se menciona un lugar o barrio donde fue encontrado, usalo en 'Ubicacion'\n"
            "• Identifica colores predominantes del pelaje con porcentaje aproximado (máximo 2 colores)\n"
            "• Usa menciones de enfermedades, tratamientos o condiciones para 'Condición de Salud Inicial'\n\n"
            
            "FORMATO DE CAMPOS:\n"
            "• 'Nombre': Nombre individual del animal (sin comas ni concatenaciones)\n"
            "• 'tipo_animal': Solo 'perro' o 'gato'\n"
            "• 'Edad': Incluir unidad ('2 años', '6 meses') - NUNCA solo números\n"
            "• 'color_pelo': Array de objetos con color y porcentaje\n"
            "• 'Condición de Salud Inicial': Estado cuando fue recibido/rescatado\n"
            "• 'Ubicacion': Lugar específico donde fue encontrado (si se menciona)\n\n"
            
            "⚠ FORMATO DE RESPUESTA:\n"
            "- NO uses bloques de código markdown\n"
            "- NO agregues texto explicativo\n"
            "- Devolvé SOLAMENTE el JSON array válido\n"
            "- La respuesta debe empezar con [ y terminar con ]\n"
            
            "EJEMPLOS VÁLIDOS:\n"
            "Un animal: [{'Nombre': 'max', 'tipo_animal': 'perro', ...}]\n"
            "Múltiples: [{'Nombre': 'luna', ...}, {'Nombre': 'sol', ...}]\n"
            "Sin animales: IGNORAR"
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
    
    def _get_caption_prompt_template(self)-> str:
        return ("""Devolvé SOLO la salida mínima indicada. Nada de texto extra.
Al final se informa la fecha de publicación  :
FECHA_PUBLICACION: 2025-08-09 19:00:00

SALIDA:
- No hay animales concretos (institucional): 0
- Hay animales: ["<nombres|sin_nombre>",[[u,e,"t",p,r],...]]

MAPEOS:
u:1 Refugio,2 Transito,3 Veterinaria,4 Hogar_adoptante
e:1 Perdido,2 En Tratamiento,3 En Adopción,5 Adoptado,6 Fallecido
r:1 Adoptante,2 Transitante,3 Veterinario,4 Voluntario,5 Interesado

NOMBRES:
- Detectar en: "Soy ___","Se llama ___","Este es ___","Conocé a ___","Info sobre ___", hashtags #Nombre, menciones @nombre.
- Normalizar: minúsculas, sin emojis/acentos/signos, trim. Diminutivos NO duplican (uva/uvita=>"uva").
- Varios animales: separar por comas SIN espacios ("luna,max").
- Si hay animales pero sin nombres claros: "sin_nombre".
- Si NO menciona nombres específicos de animales Y es contenido institucional (donaciones generales, rifas, anuncios sin animales específicos): devolver 0.
- Si menciona un nombre específico de animal (aunque sea solo pidiendo ayuda): SÍ es contenido concreto.

EVENTOS (cada item = [u,e,"t",p,r]):
- Una sola acción/estado => un solo item. Historia o "ACTUALIZACIÓN:" => múltiples en orden cronológico.
- Prioridad de estado si hay señales simultáneas: 6>5>3>2>1.
- "ayudar a [nombre]"/"ayudanos a ayudar"/"necesita ayuda" (sin mencionar adopción) => e=2 (En Tratamiento), u=1 (Refugio).
Reglas:
- Si menciona animal específico Y pide ayuda/colaboración Y NO dice "en adopción" => e=2 (En Tratamiento)
- Si dice explícitamente "en adopción" => e=3 (En Adopción)  
- Si dice "adoptado" => e=5 (Adoptado)
-"rescatados" + "hace casi 2 meses" → debería crear evento de rescate
-"en adopción" → debería crear evento de adopción
- “tratamiento/medicación/operación” => e=2 (no adopción).
- “buscamos/necesitamos tránsito” y no dice que esté en adopción => e=2 (ubicación por defecto u=1 si no hay otra).
- “adoptado/adoptada” => e=5 y u=4.
- Ubicación: u=2 SOLO con evidencia (“en tránsito”/“hogar temporal”/“con [persona]”); u=3 SOLO si internado/hospitalizado/“queda en clínica/guardia 24h”; si no hay señal, u=1.
- Hashtags imperativos (#transita,#adopta) NO cambian u/e por sí solos.

PERSONA p y RELACIÓN r:
- Extraer si el texto asocia claramente persona o cuenta de ig con el evento:
  - Tránsito: “en tránsito con X/@X”, “gracias a X/@X por transitar”, “con X” ⇒ r=2. Si es mas de una persona separar por coma los nombres, por ejemplo "Carli y Fran" "Carli, Fran"
  - Adopción: “adoptado por X”, “gracias X por adoptarlo” ⇒ r=1.
  - Veterinaria: “Dra./Dr./clínica ___”, “queda internado en ___” ⇒ r=3.
  - Voluntario: “gracias X por rescatar/trasladar/alojar” (si no es tránsito/adopción) ⇒ r=4.
  - Interesado: solo si nombra a alguien como interesado específico ⇒ r=5.
 
FECHA ABSOLUTA "t" (derivada de FECHA_PUBLICACION):
- Si el evento ocurre “hoy” ⇒ usar EXACTAMENTE la FECHA_PUBLICACION en formato "DD/MM/AAAA HH:MM:SS".
- “ayer”/“anoche” ⇒ FECHA_PUBLICACION - 1 día (misma hora).
- “anteayer” ⇒ -2 días.
- “hace X días/horas/semanas/meses/años” ⇒ restar esa cantidad desde FECHA_PUBLICACION (semana≈7, mes≈30, año≈365) conservando la hora; si se puede, restar meses/años en calendario (mismo día de mes; si no existe, último día).
- "hace 2 meses" desde 06/06/2025 = 07/04/2025
- "hace casi 2 meses" debería interpretarse como "hace 2 meses"
- Fechas absolutas en el texto (DD/MM[/AAAA] o “DD de <mes>”): convertirlas a "DD/MM/AAAA HH:MM:SS" usando la hora de FECHA_PUBLICACION si el texto no da hora.
- Si no hay pista temporal para ese evento ⇒ "".
- "hace casi X tiempo" = tratar como "hace X tiempo"

FORMATO:
- Responder EXACTAMENTE 0 o ["<nombres|sin_nombre>",[[u,e,"t","p",r],...]] sin espacios ni saltos de línea.
""")