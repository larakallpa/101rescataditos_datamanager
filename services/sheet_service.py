# src/services/sheet_service.py
"""
Google Sheets Service Module

This module handles interactions with Google Sheets for data storage and retrieval.
"""
import os
import logging
import gspread
from datetime import datetime
from typing import Dict, List, Any, Optional, Union
from oauth2client.service_account import ServiceAccountCredentials

logger = logging.getLogger(__name__)

class SheetService:
    """Handles operations with Google Sheets API."""
    
    def __init__(self):
        """Initialize Google Sheets client with credentials."""
        try:
            # Set up credentials
            scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
            self.creds = ServiceAccountCredentials.from_json_keyfile_name("credenciales.json", scopes=scope)
            self.client = gspread.authorize(self.creds)
             
            # Open the spreadsheet using the key from environment variables
            spreadsheet_key = os.getenv("KEY_SHEET")
            self.spreadsheet = self.client.open_by_key(spreadsheet_key)
            
            logger.info("Google Sheets service initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize Google Sheets service: {str(e)}")
            raise
    
    def get_worksheet(self, worksheet_name: str) -> gspread.worksheet.Worksheet:
        """Get a worksheet by name.
        
        Args:
            worksheet_name: Name of the worksheet
            
        Returns:
            The worksheet object
            
        Raises:
            ValueError: If worksheet is not found
        """
        try:
            worksheet = self.spreadsheet.worksheet(worksheet_name)
            return worksheet
        except Exception as e:
            logger.error(f"Failed to get worksheet '{worksheet_name}': {str(e)}")
            raise ValueError(f"Worksheet '{worksheet_name}' not found")
    
    def get_headers(self, worksheet: gspread.worksheet.Worksheet) -> List[str]:
        """Get the header row from a worksheet.
        
        Args:
            worksheet: The worksheet to get headers from
            
        Returns:
            List of column headers
        """
        try:
            return worksheet.row_values(1)
        except Exception as e:
            logger.error(f"Failed to get headers: {str(e)}")
            return []
    
    def get_oldest_date(self, worksheet: gspread.worksheet.Worksheet) -> datetime:
        """Get the oldest date in the spreadsheet for comparison.
        
        Args:
            worksheet: The worksheet to search in
            
        Returns:
            The oldest date found or a default date (2020-01-01)
        """
        try:
            headers = self.get_headers(worksheet)
            
            # Find the date column index
            date_column_index = -1
            for idx, header in enumerate(headers):
                if header.lower() in ["fecha", "date"]:
                    date_column_index = idx + 1
                    break
            
            if date_column_index == -1:
                logger.warning("Date column not found in worksheet")
                return datetime(2020, 1, 1, 0, 0, 0)
            
            # Get all date values (excluding header)
            date_values = worksheet.col_values(date_column_index)[1:]
            
            # Convert to datetime objects
            dates = []
            for date_str in date_values:
                if date_str.strip():
                    try:
                        # Handle different date formats
                        if "/" in date_str:
                            dt = datetime.strptime(date_str, "%d/%m/%Y %H:%M:%S")
                            
                        else:
                            dt = datetime.strptime(date_str, "%d/%m/%Y %H:%M:%S")
                        dates.append(dt)
                    except ValueError:
                        logger.warning(f"Could not parse date: {date_str}")
            
            if dates:
                return max(dates)
            else:
                return datetime(2020, 1, 1, 0, 0, 0)
                
        except Exception as e:
            logger.error(f"Error finding oldest date: {str(e)}")
            return datetime(2020, 1, 1, 0, 0, 0)
    
    def find_row_by_id(self, worksheet: gspread.worksheet.Worksheet, id: int) -> int:
        """Find a row by name in the worksheet.
        
        Args:
            worksheet: The worksheet to search in
            name: Name to search for
            
        Returns:
            Row index (1-based) or -1 if not found
        """
        try:
            headers = self.get_headers(worksheet)
            
            # Find the name column index
            name_col_index = -1
            for idx, header in enumerate(headers):
                if header.lower() == "id":
                    name_col_index = idx + 1
                    break
            
            if name_col_index == -1:
                logger.warning("Name column not found in worksheet")
                return -1
            
            # Get all name values
            name_values = worksheet.col_values(name_col_index)
            
            # Find the matching row (case-insensitive)
            for row_idx, cell_value in enumerate(name_values):
                if cell_value== id:
                    return row_idx + 1  # Convert to 1-based indexing
            
            return -1  # Not found
            
        except Exception as e:
            logger.error(f"Error finding row by name '{id}': {str(e)}")
            return -1
        
        
    def get_oldest_id(self, worksheet: gspread.worksheet.Worksheet) -> int:
        """Get the oldest id in the spreadsheet for comparison.
        
        Args:
            worksheet: The worksheet to search in
            
        Returns:
            The oldest id found or a default 1
        """
        try:
            headers = self.get_headers(worksheet)
            
            # Find the date column index
            id_column_index = -1
            for idx, header in enumerate(headers):
                if header.lower() =="id" :
                    id_column_index = idx + 1
                    break
            
            if id_column_index == -1:
                logger.warning("Id column not found in worksheet")
                return 1
            
            # Get all date values (excluding header)
            id_values = worksheet.col_values(id_column_index)[1:]
            

            id_numbers = [int(val) for val in id_values if val.strip().isdigit()]
 

            # Convert to datetime objects
 
            if id_numbers:
                return max(id_numbers)
            else:
                return 1
                
        except Exception as e:
            logger.error(f"Error finding oldest date: {str(e)}")
            return 1

    
    def insert_sheet_from_dict(self,data: Union[Dict[str, Any], List[Dict[str, Any]]],worksheet: gspread.worksheet.Worksheet) -> bool:
        """create a row in the worksheet based on dictionary data.
        Args:
            data: Dictionary with data to update or list of dictionaries
            worksheet: The worksheet to update
            
        Returns:
            True if successful, False otherwise
        """
        # Handle list of dictionaries
        if isinstance(data, list):
            success = True
            for item in data:
                if not self.insert_sheet_from_dict(item, worksheet):
                    success = False
            return success
        
        try:
            headers = self.get_headers(worksheet)
            logger.info(f"Creating new entry for {data}")
                
                # Create a list with values in the correct order
            new_row = []
            for header in headers: 
                new_row.append(data.get(header, ""))
                
                # Append the new row
            worksheet.append_row(new_row)
            print("insertado ")
            return True
            
        except Exception as e:
            logger.error(f"Error updating sheet: {str(e)}")
            return False

    def get_id(self, name: str, worksheet: gspread.worksheet.Worksheet) -> int | None:
        """
        Versión que devuelve None si no encuentra el valor (sin excepciones)
        """
        try:
            values = worksheet.get_all_values()
            if not values:
                return None
                
            headers = [h.lower().strip() for h in values[0]]
            
            try:
                name_col = headers.index('nombre')
                id_col = headers.index('id')
            except ValueError:
                return None
            
            for row in values[1:]:
                if (len(row) > name_col and 
                    row[name_col].strip().lower() == name.strip().lower()):
                    if len(row) > id_col and row[id_col].strip():
                        return row[id_col].strip()
            
            return None
            
        except Exception:
            return None


    def batch_update( self,worksheet: gspread.worksheet.Worksheet, data: List[Dict[str, Any]]) -> bool:
        """Perform a batch update of multiple rows for better performance.
        
        Args:
            worksheet: The worksheet to update
            data: List of dictionaries with data to update
            
        Returns:
            True if successful, False otherwise
        """
        logger.info(f"Actualizando {data}")
        try:
            if not data:
                return True
                
            # Get headers to ensure data aligns with columns
            headers = self.get_headers(worksheet)
            
            # Prepare batch update requests
            batch_requests = []
            
            for item in data:
                # Skip items without name
                if "ID" not in item:
                    continue
                    
                id_to_find = item["ID"]
                row_idx = self.find_row_by_id(worksheet, id_to_find)
                
                if row_idx > 1:  # Found existing row
                    # Create update requests for each field
                    for key, value in item.items():
                        if key in headers:
                            col_idx = headers.index(key) + 1
                            batch_requests.append({
                                'range': f"{gspread.utils.rowcol_to_a1(row_idx, col_idx)}",
                                'values': [[value]]
                            })
                else:
                    # New row, prepare in correct order
                    new_row = []
                    for header in headers:
                        new_row.append(item.get(header, ""))
                    
                    # Add as append request
                    batch_requests.append({
                        'range': f"A{worksheet.row_count + 1}:{gspread.utils.rowcol_to_a1(1, len(headers))}",
                        'values': [new_row]
                    })
            
            # Execute batch update
            if batch_requests:
                worksheet.batch_update(batch_requests)
                
            return True
            
        except Exception as e:
            logger.error(f"Error in batch update: {str(e)}")
            return False
    
    def buscar_valor_en_fila(self,worksheet, columna_busqueda, columna_retorno, valor_buscado):
        """
        Busca un valor en una columna y devuelve el valor correspondiente de otra columna en la misma fila.
        
        Args:
            worksheet: Hoja de cálculo de gspread.
            columna_busqueda (str): Nombre de la columna donde buscar el valor.
            columna_retorno (str): Nombre de la columna cuyo valor se desea obtener.
            valor_buscado (str/int): Valor que se desea buscar.
        
        Returns:
            str: Valor correspondiente de la columna de retorno o None si no se encuentra.
        """
        try:
            headers = worksheet.row_values(1)
            
            # Buscar índice de las columnas
            idx_busqueda = next((i for i, h in enumerate(headers) if h.lower() == columna_busqueda.lower()), -1)
            idx_retorno = next((i for i, h in enumerate(headers) if h.lower() == columna_retorno.lower()), -1)
            
            if idx_busqueda == -1 or idx_retorno == -1:
                print("No se encontraron las columnas indicadas.")
                return None
            
            # Obtener todas las filas (excluyendo encabezados)
            filas = worksheet.get_all_values()[1:]
            
            for fila in filas:
                if len(fila) > idx_busqueda and str(fila[idx_busqueda]).strip() == str(valor_buscado):
                    if len(fila) > idx_retorno:
                        return fila[idx_retorno]
                    else:
                        return None
            
            return None
        
        except Exception as e:
            print(f"Error al buscar el valor: {e}")
            return None
    def get_estado(self,  post_id):
        worksheet_post = self.get_worksheet( "Post")
        id = self.buscar_valor_en_fila(worksheet_post, "id_post", "id", post_id)
        worksheet_datos = self.get_worksheet( "Datos")
        estado_actual = self.buscar_valor_en_fila(worksheet_datos, "id", "Estado Actual", id)
        return estado_actual