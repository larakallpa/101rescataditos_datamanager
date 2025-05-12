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
            spreadsheet_key = os.getenv("SPREADSHEET_KEY")
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
                            dt = datetime.strptime(date_str.split(" ")[0], "%d/%m/%Y")
                        else:
                            dt = datetime.strptime(date_str.split(" ")[0], "%Y-%m-%d")
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
    
    def find_row_by_name(self, worksheet: gspread.worksheet.Worksheet, name: str) -> int:
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
                if header.lower() in ["nombre", "name"]:
                    name_col_index = idx + 1
                    break
            
            if name_col_index == -1:
                logger.warning("Name column not found in worksheet")
                return -1
            
            # Get all name values
            name_values = worksheet.col_values(name_col_index)
            
            # Find the matching row (case-insensitive)
            for row_idx, cell_value in enumerate(name_values):
                if cell_value.lower() == name.lower():
                    return row_idx + 1  # Convert to 1-based indexing
            
            return -1  # Not found
            
        except Exception as e:
            logger.error(f"Error finding row by name '{name}': {str(e)}")
            return -1
    
    def update_sheet_from_dict(self,data: Union[Dict[str, Any], List[Dict[str, Any]]],worksheet: gspread.worksheet.Worksheet) -> bool:
        """Update or create a row in the worksheet based on dictionary data.
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
                if not self.update_sheet_from_dict(item, worksheet):
                    success = False
            return success
        
        try:
            headers = self.get_headers(worksheet)
            
            # Check if data has a name for lookup
            if "Nombre" not in data:
                logger.warning("Data missing 'Nombre' key for lookup")
                return False
            
            # Try to find existing row
            name_to_find = data["Nombre"]

            row_idx = self.find_row_by_name(worksheet, name_to_find)
                
            if row_idx > 1 and name_to_find and name_to_find.strip():  # Found (and not header row)
                logger.info(f"Updating existing entry for '{name_to_find}' at row {row_idx}")
                
                # Get current row data to compare
                current_row_data = worksheet.row_values(row_idx)
                
                # Update each field only if new data has more information
                for key, value in data.items():
                    if key in headers:
                        col_idx = headers.index(key) + 1
                        
                        # Obtener el valor actual en la celda
                        current_value = current_row_data[col_idx-1] if col_idx-1 < len(current_row_data) else ""
                        
                        # Solo actualizar si el nuevo valor tiene más información que el actual
                        if value and (not current_value or len(str(value)) > len(str(current_value))):
                            worksheet.update_cell(row_idx, col_idx, value)
                            logger.debug(f"Updated {key} from '{current_value}' to '{value}'")
                            
            else:  # Not found, create new row
                logger.info(f"Creating new entry for '{name_to_find}'")
                
                # Create a list with values in the correct order
                new_row = []
                for header in headers:
                    new_row.append(data.get(header, ""))
                
                # Append the new row
                worksheet.append_row(new_row)

            return True
            
        except Exception as e:
            logger.error(f"Error updating sheet: {str(e)}")
            return False
    
    def batch_update(
        self, 
        worksheet: gspread.worksheet.Worksheet, 
        data: List[Dict[str, Any]]
    ) -> bool:
        """Perform a batch update of multiple rows for better performance.
        
        Args:
            worksheet: The worksheet to update
            data: List of dictionaries with data to update
            
        Returns:
            True if successful, False otherwise
        """
        try:
            if not data:
                return True
                
            # Get headers to ensure data aligns with columns
            headers = self.get_headers(worksheet)
            
            # Prepare batch update requests
            batch_requests = []
            
            for item in data:
                # Skip items without name
                if "Nombre" not in item:
                    continue
                    
                name_to_find = item["Nombre"]
                row_idx = self.find_row_by_name(worksheet, name_to_find)
                
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