import os
import re
import logging
import pandas as pd
import pdfplumber
import glob
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import numpy as np
from typing import List, Dict, Any, Optional
from dataclasses import dataclass
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("transaction_processor.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("TransactionProcessor")

# Configuration
@dataclass
class Config:
    # File paths
    PDF_FOLDER = r"C:\Users\guisell.lara\Documents\cargarExcelxvoz\pdftransferencias"
    EXCEL_FOLDER = r"C:\Users\guisell.lara\Documents\cargarExcelxvoz\transacciones"
    OUTPUT_FOLDER = r"C:\Users\guisell.lara\Documents\cargarExcelxvoz"
    CREDENTIALS_FILE = "credenciales.json"
    
    # Google Sheet settings
    SPREADSHEET_ID = "1UvIvxfEejGRBb7Cc_WWyYlQF7s0k__E29omKMv_ebpE"
    WORKSHEET_NAME = "Transaccion donaciones"
    
    # Transaction processing
    EXCLUDED_NAMES = {
        "Geraldine Nicole Lara Arteaga",
        "Geraldine Lara Arteaga",
        "Gisell lara",
        "Guisell Margarita Lara Arteaga",
        "LARA ARTEAGA GERALDINE NICOLE",
        "Lara Arteaga, Geraldine Nicole",
        "Lara Arteaga Geraldine Nicole",
        "Ricardo Julio Lara Espinoza",
        "Eloy Edison Arrelucea Arteaga",
        "Arteaga Curahua, Lus Marilda", 
        "Emiliano David Garcia Perez",
        "Marquez Coronel, Melody Ayelen",
        "GERALDINE NICO ARTEAGA",
        "MARQUEZ CORONEL MELODY AYELEN"
    }
    
    # Keywords for expense filtering
    EXPENSE_KEYWORDS = ['VETERINA', 'LINARES', 'MASCOTAS', 'PET', 'POPPI', 
                         'WALTER EDUARDO PEREZ', 'CABIFY', 'BALANCEADOS']
    
    # Column mappings
    MAPPING_COLUMNS = {
        'ID DE OPERACIÓN EN MERCADO PAGO': 'ID de la operación',
        'FECHA DE ORIGEN': 'Fecha',
        'MEDIO DE PAGO': 'MEDIO DE PAGO',
        'TIPO DE IDENTIFICACIÓN DEL PAGADOR': 'TIPO DE IDENTIFICACIÓN DEL PAGADOR',
        'NÚMERO DE IDENTIFICACIÓN DEL PAGADOR': 'NÚMERO DE IDENTIFICACIÓN DEL PAGADOR',
        'PAGADOR': 'PAGADOR',
        'DETALLE DE LA VENTA': 'DETALLE DE LA VENTA'
    }


class PDFExtractor:
    """Class to extract financial transactions from PDF files."""
    
    def __init__(self):
        self.transfer_patterns = [
            # Primary pattern
            r'(\d{2}-\d{2}-\d{4})\s+(Transferencia\s+.+?)\s+(\d{9,})\s+\$\s*(-?[\d.,]+)(?:\s+\$\s*[\d.,]+)?',
            # Date + description pattern
            r'(\d{2}-\d{2}-\d{4})\s+(Transferencia\s+.+?)(?:\s{2,}|\d{9,}|$)',
            # Description-only pattern
            r'(Transferencia\s+.+?)$'
        ]
        
        self.payment_patterns = [
            # Primary payment pattern
            r'(\d{2}-\d{2}-\d{4})\s+(Pago\s+.+?)\s+(\d{9,})\s+\$\s*(-?[\d.,]+)(?:\s+\$\s*[\d.,]+)?'
        ]
        
        self.payment_keywords = ['Balanceados', 'Pet', 'Poppi', 'VETERINARIA', 'CABIFY']
    
    def extract_from_file(self, pdf_path: str) -> List[Dict[str, Any]]:
        """Extract transactions from a PDF file."""
        logger.info(f"Extracting transactions from {pdf_path}")
        transactions = []
        
        try:
            with pdfplumber.open(pdf_path) as pdf:
                for page_num, page in enumerate(pdf.pages, 1):
                    logger.debug(f"Processing page {page_num}")
                    text = page.extract_text() or ""
                    lines = text.split('\n')
                    
                    i = 0
                    while i < len(lines):
                        line = lines[i]
                        
                        # Process transfers
                        if 'Transferencia' in line:
                            tx = self._extract_transfer(lines, i)
                            if tx:
                                transactions.append(tx)
                        
                        # Process payments with keywords
                        elif 'Pago ' in line and any(kw in line for kw in self.payment_keywords):
                            tx = self._extract_payment(lines, i)
                            if tx:
                                transactions.append(tx)
                        
                        i += 1
        except Exception as e:
            logger.error(f"Error extracting from PDF {pdf_path}: {e}")
        
        return transactions
    
    def _extract_transfer(self, lines: List[str], line_idx: int) -> Optional[Dict[str, Any]]:
        """Extract transfer transaction from lines starting at line_idx."""
        line = lines[line_idx]
        
        # Try primary pattern
        for pattern in self.transfer_patterns:
            match = re.search(pattern, line)
            if match and len(match.groups()) >= 3:
                try:
                    fecha = match.group(1)
                    description = match.group(2)
                    
                    # Handle different pattern group counts
                    if len(match.groups()) >= 4:
                        op_id = match.group(3)
                        value_str = match.group(4)
                        value = float(value_str.replace('.', '').replace(',', '.'))
                    else:
                        # Look for ID and value in next line
                        if line_idx + 1 < len(lines):
                            next_line = lines[line_idx + 1]
                            id_match = re.search(r'(\d{9,})', next_line)
                            value_match = re.search(r'\$\s*(-?[\d.,]+)', next_line)
                            
                            op_id = id_match.group(1) if id_match else "Unknown"
                            if value_match:
                                value_str = value_match.group(1)
                                value = float(value_str.replace('.', '').replace(',', '.'))
                            else:
                                continue
                    
                    return {
                        'Fecha': fecha,
                        'Descripción': description,
                        'ID de la operación': op_id,
                        'Valor': value
                    }
                except (IndexError, ValueError) as e:
                    logger.debug(f"Partial match failed: {e}")
                    continue
        
        # Try multi-line extraction (3-line strategy)
        if line_idx + 1 < len(lines):
            desc_match = re.search(r'(Transferencia\s+.+?)$', line)
            if desc_match:
                description = desc_match.group(1).strip()
                next_line = lines[line_idx + 1]
                parts_match = re.search(r'(\d{2}-\d{2}-\d{4})\s+(\d{9,})\s+\$\s*(-?[\d.,]+)', next_line)
                
                if parts_match:
                    fecha, op_id, value_str = parts_match.groups()
                    value = float(value_str.replace('.', '').replace(',', '.'))
                    
                    # Check if description continues on third line
                    if line_idx + 2 < len(lines):
                        third_line = lines[line_idx + 2].strip()
                        if (not re.match(r'^\d{2}-\d{2}-\d{4}', third_line) and 
                            not re.search(r'\d{9,}', third_line)):
                            description += ' ' + third_line
                    
                    return {
                        'Fecha': fecha,
                        'Descripción': description,
                        'ID de la operación': op_id,
                        'Valor': value
                    }
        
        return None
    
    def _extract_payment(self, lines: List[str], line_idx: int) -> Optional[Dict[str, Any]]:
        """Extract payment transaction from lines starting at line_idx."""
        line = lines[line_idx]
        
        for pattern in self.payment_patterns:
            match = re.search(pattern, line)
            if match:
                try:
                    fecha, description, op_id, value_str = match.groups()
                    value = float(value_str.replace('.', '').replace(',', '.'))
                    
                    return {
                        'Fecha': fecha,
                        'Descripción': description,
                        'ID de la operación': op_id,
                        'Valor': value
                    }
                except (IndexError, ValueError) as e:
                    logger.debug(f"Payment match failed: {e}")
        
        return None


class ExcelProcessor:
    """Class to process and merge Excel files."""
    
    def merge_excel_files(self, excel_folder: str, output_name: str = "transaccion_unificadas.xlsx") -> Optional[str]:
        """Merge all Excel files in the specified folder into a single Excel file."""
        logger.info(f"Merging Excel files from {excel_folder}...")
        
        # Get all Excel files in the folder
        excel_files = glob.glob(os.path.join(excel_folder, "*.xlsx"))
        excel_files.extend(glob.glob(os.path.join(excel_folder, "*.xls")))
        
        if not excel_files:
            logger.warning("No Excel files found in the specified folder.")
            return None
        
        # Merge all Excel files
        combined_df = pd.DataFrame()
        
        for excel_file in excel_files:
            try:
                # Define data types for specific columns
                dtypes = {'ID DE OPERACIÓN EN MERCADO PAGO': str}
                
                try:
                    df = pd.read_excel(excel_file, dtype=dtypes)
                except Exception:
                    # If that fails, read normally then convert after
                    df = pd.read_excel(excel_file)
                    
                    # Look for ID columns and convert them to string
                    id_columns = [col for col in df.columns if 'ID' in col.upper() 
                                 and ('OPERACI' in col.upper() or 'MERCADO' in col.upper())]
                    for col in id_columns:
                        df[col] = df[col].astype(str)
                
                combined_df = pd.concat([combined_df, df], ignore_index=True)
            except Exception as e:
                logger.error(f"Error reading {excel_file}: {e}")
        
        if combined_df.empty:
            logger.warning("No data found in Excel files.")
            return None
        
        # Ensure ID columns are strings in combined dataframe
        id_columns = [col for col in combined_df.columns if 'ID' in col.upper() 
                     and ('OPERACI' in col.upper() or 'MERCADO' in col.upper())]
        for col in id_columns:
            combined_df[col] = combined_df[col].astype(str)
            logger.info(f"Ensuring column '{col}' is stored as text")
        
        # Save the combined DataFrame to a new Excel file with string preservation
        output_path = os.path.join(excel_folder, output_name)
        
        # If file exists, remove it
        if os.path.isfile(output_path):
            os.remove(output_path)
        
        # Create an Excel writer with the xlsxwriter engine
        with pd.ExcelWriter(output_path, engine='xlsxwriter', mode='w') as writer:
            combined_df.to_excel(writer, index=False, sheet_name='Sheet1')
            
            # Get the xlsxwriter workbook and worksheet objects
            workbook = writer.book
            worksheet = writer.sheets['Sheet1']
            
            # Create a text format for ID columns
            text_format = workbook.add_format({'num_format': '@'})
            
            # Apply text format to ID columns
            for col_idx, col_name in enumerate(combined_df.columns):
                if 'ID' in col_name.upper() and ('OPERACI' in col_name.upper() or 'MERCADO' in col_name.upper()):
                    worksheet.set_column(col_idx, col_idx, None, text_format)
        
        logger.info(f"Merged {len(excel_files)} Excel files with {len(combined_df)} rows to {output_path}")
        return output_path


class DataCombiner:
    """Class to combine and process transaction data."""
    
    def __init__(self, config: Config):
        self.config = config
    
    def clean_description(self, description):
        """Clean transaction descriptions."""
        if not isinstance(description, str):
            return description
            
        prefixes = ("Transferencia recibida", "Transferencia enviada", "Transferencia", "Pago")
        for prefix in prefixes:
            if description.startswith(prefix):
                return description[len(prefix):].strip()
        return description
    
    def create_final_excel(self, pdf_extracts_path: str, unified_excel_path: str) -> str:
        """Create final Excel files by combining PDF extracts and unified Excel data."""
        logger.info("Creating final Excel files...")
        
        # Read source data
        pdf_data = pd.read_excel(pdf_extracts_path)
        unified_data = pd.read_excel(unified_excel_path)
        
        # Remove cancelled transfers
        pdf_data = pdf_data[~pdf_data['Descripción'].str.contains('Transferencia cancelada', 
                                                               case=False, na=False)]
        
        # Clean descriptions
        pdf_data['Descripción'] = pdf_data['Descripción'].apply(self.clean_description)
        
        # Normalize data types
        pdf_data['ID de la operación'] = pdf_data['ID de la operación'].astype(str)
        unified_data["ID DE OPERACIÓN EN MERCADO PAGO"] = unified_data["ID DE OPERACIÓN EN MERCADO PAGO"].astype(str)
        
        # Map column names between dataframes
        cols_map = {}
        for key, val in self.config.MAPPING_COLUMNS.items():
            for col in unified_data.columns:
                if col.upper() == key:
                    cols_map[key] = col
                    break
        
        # Build final dataset
        final_rows = self._build_combined_data(pdf_data, unified_data, cols_map)
        df = pd.DataFrame(final_rows)
        
        # Add DONANTE column
        df['DONANTE'] = df['Descripción'].where(df['Descripción'].str.strip() != "", df['PAGADOR'])
        
        # Exclude unwanted names
        df = df[~df['DONANTE'].isin(self.config.EXCLUDED_NAMES)]
        
        # Select final columns
        df_final = df[[
            'ID DE LA OPERACION',
            'FECHA',
            'MEDIO DE PAGO',
            'TIPO DE IDENTIFICACIÓN DEL PAGADOR',
            'NÚMERO DE IDENTIFICACIÓN DEL PAGADOR',
            'DONANTE',
            'DETALLE DE LA VENTA',
            'VALOR'
        ]]
        
        # Clean up data
        df_final = df_final.replace([np.inf, -np.inf], np.nan)
        df_final = df_final.fillna('')
        
        # Split positive and negative transactions
        df_positivos = df_final[df_final['VALOR'] >= 0].copy()
        df_negativos = df_final[df_final['VALOR'] < 0].copy()
        df_negativos['VALOR'] = df_negativos['VALOR'].abs()
        
        # Process expense data
        expenses_df = self._process_expenses(df_negativos)
        
        # Save expenses to Excel
        negativos_path = os.path.join(self.config.OUTPUT_FOLDER, "transacciones_negativas.xlsx")
        expenses_df.to_excel(negativos_path, index=False)
        
        logger.info(f"{len(df_positivos)} positive transactions ready for Google Sheets")
        logger.info(f"{len(expenses_df)} expense transactions saved to {negativos_path}")
        
        return negativos_path, df_positivos
    
    def _build_combined_data(self, pdf_data, unified_data, cols_map):
        """Build combined dataset from PDF and Excel data."""
        final_rows = []
        
        for _, pdf_row in pdf_data.iterrows():
            pid = pdf_row['ID de la operación']
            matches = unified_data[unified_data["ID DE OPERACIÓN EN MERCADO PAGO"] == pid]
            uni = matches.iloc[0].to_dict() if not matches.empty else {}
            
            # Get date from best source
            fecha_raw = uni.get(cols_map.get('FECHA DE ORIGEN'))
            if pd.isna(fecha_raw):
                fecha_raw = pdf_row.get('Fecha')
            
            # Convert to datetime and format
            fecha_ts = pd.to_datetime(fecha_raw, errors="coerce")
            fecha = fecha_ts.strftime("%d/%m/%Y %H:%M:%S") if not pd.isna(fecha_ts) else ""
            
            # Get other fields
            medio = uni.get(cols_map.get('MEDIO DE PAGO'), "")
            tipo_id = uni.get(cols_map.get('TIPO DE IDENTIFICACIÓN DEL PAGADOR'), "")
            num_id = uni.get(cols_map.get('NÚMERO DE IDENTIFICACIÓN DEL PAGADOR'), "")
            pagador = uni.get(cols_map.get('PAGADOR'), "")
            detalle = uni.get(cols_map.get('DETALLE DE LA VENTA'), "")
            
            final_rows.append({
                'ID DE LA OPERACION': pid,
                'FECHA': fecha,
                'MEDIO DE PAGO': medio,
                'TIPO DE IDENTIFICACIÓN DEL PAGADOR': tipo_id,
                'NÚMERO DE IDENTIFICACIÓN DEL PAGADOR': num_id,
                'PAGADOR': pagador,
                'DETALLE DE LA VENTA': detalle,
                'Descripción': pdf_row['Descripción'],
                'VALOR': pdf_row.get('Valor')
            })
        
        return final_rows
    
    def _process_expenses(self, df_negativos):
        """Process negative transactions for expense tracking."""
        # Filter expenses based on keywords
        pattern = '|'.join(map(re.escape, self.config.EXPENSE_KEYWORDS))
        df = df_negativos[df_negativos['DONANTE'].str.upper().str.contains(pattern, regex=True, na=False)].copy()
        
        # Convert date column for filtering
        df['FECHA'] = pd.to_datetime(df['FECHA'], format="%d/%m/%Y %H:%M:%S", errors='coerce')
        
        # Filter Cabify transactions - keep only weekend rides
        mask_cabify = df['DONANTE'].str.upper().str.contains(r'\bCABIFY\b', na=False)
        mask_weekend = df['FECHA'].dt.weekday.isin([5, 6])  # sábado=5, domingo=6
        df = df[~(mask_cabify & ~mask_weekend)].copy()
        
        # Rename columns
        df = df.rename(columns={
            'DONANTE': 'Nombre de Proveedor',
            'DETALLE DE LA VENTA': 'DETALLE',
            'VALOR': 'MONTO',
            'ID DE LA OPERACION': 'Observacion'
        })
        
        # Calculate expense type based on provider name
        conditions = [
            df['Nombre de Proveedor'].str.upper().str.contains(r'\b(?:WALTER EDUARDO PEREZ|VETERINAR|LINARES, MARCELO)\b'),
            df['Nombre de Proveedor'].str.upper().str.contains(r'\bCABIFY\b')
        ]
        df['Tipo de gasto'] = np.select(conditions, ['Veterinaria', 'Transporte'], default='Alimentos')
        
        # Add fixed columns
        df['MASCOTA'] = ''
        df['RESPONSABLE'] = 'LARA GERALDINE'
        df['FORMA DE PAGO'] = 'MERCADOPAGO'
        df['Tiempo Total (con traslados)'] = ''
        df['foto'] = ''
        
        # Select and order columns
        return df[[
            'FECHA',
            'Nombre de Proveedor',
            'Tipo de gasto',
            'MASCOTA',
            'RESPONSABLE',
            'DETALLE',
            'MONTO',
            'FORMA DE PAGO',
            'Observacion',
            'Tiempo Total (con traslados)',
            'foto'
        ]]


class GoogleSheetsUploader:
    """Class to upload data to Google Sheets."""
    
    def __init__(self, config: Config):
        self.config = config
    
    def upload_to_sheets(self, df: pd.DataFrame) -> bool:
        """Upload a DataFrame to Google Sheets."""
        logger.info("Uploading data to Google Sheets...")
        
        try:
            # Define permissions
            scope = [
                'https://spreadsheets.google.com/feeds',
                'https://www.googleapis.com/auth/spreadsheets',
                'https://www.googleapis.com/auth/drive'
            ]
            
            # Get credentials
            credentials = ServiceAccountCredentials.from_json_keyfile_name(
                self.config.CREDENTIALS_FILE, scopes=scope)
            
            # Authorize
            gc = gspread.authorize(credentials)
            
            # Open spreadsheet
            spreadsheet = gc.open_by_key(self.config.SPREADSHEET_ID)
            worksheet = spreadsheet.worksheet(self.config.WORKSHEET_NAME)
            
            # Get existing data
            existing_data = worksheet.get_all_values()
            
            if len(existing_data) > 0:
                # Convert DataFrame to list of lists
                valores = df.values.tolist()
                
                # Check if there are only headers
                if len(existing_data) == 1:
                    worksheet.append_rows(valores)
                else:
                    # Check if headers match
                    headers_exist = existing_data[0]
                    if headers_exist == df.columns.tolist():
                        worksheet.append_rows(valores)
                    else:
                        logger.warning("Headers don't match exactly, appending anyway")
                        worksheet.append_rows(valores)
            else:
                # If sheet is empty, include headers
                headers = df.columns.tolist()
                all_values = [headers] + df.values.tolist()
                worksheet.append_rows(all_values)
                
            logger.info(f"Successfully uploaded {len(df)} rows to Google Sheets")
            return True
            
        except Exception as e:
            logger.error(f"Error uploading to Google Sheets: {e}")
            return False


class TransactionProcessor:
    """Main class to coordinate the transaction processing workflow."""
    
    def __init__(self):
        self.config = Config()
        self.pdf_extractor = PDFExtractor()
        self.excel_processor = ExcelProcessor()
        self.data_combiner = DataCombiner(self.config)
        self.sheets_uploader = GoogleSheetsUploader(self.config)
    
    def process(self):
        """Run the full transaction processing workflow."""
        logger.info("Starting transaction processing workflow")
        
        # Step 1: Extract transactions from PDFs
        pdf_transactions = self._extract_from_pdfs()
        if not pdf_transactions:
            logger.error("No PDF transactions found. Exiting.")
            return False
        
        # Step 2: Merge Excel files
        unified_excel_path = self._merge_excel_files()
        if not unified_excel_path:
            logger.error("Failed to create unified Excel file. Exiting.")
            return False
        
        # Step 3: Create final Excel files
        negativos_path, positivos_df = self.data_combiner.create_final_excel(
            pdf_transactions, unified_excel_path)
        
        # Step 4: Upload positive transactions to Google Sheets
        if not positivos_df.empty:
            self.sheets_uploader.upload_to_sheets(positivos_df)
        
        logger.info("Transaction processing completed successfully!")
        return True
    
    def _extract_from_pdfs(self) -> Optional[str]:
        """Extract transactions from all PDFs in the folder."""
        # Get all PDF files in the folder
        pdf_files = glob.glob(os.path.join(self.config.PDF_FOLDER, "*.pdf"))
        
        if not pdf_files:
            logger.warning("No PDF files found in the specified folder.")
            return None
        
        # Extract transactions from all PDFs
        all_transactions = []
        
        for pdf_file in pdf_files:
            transactions = self.pdf_extractor.extract_from_file(pdf_file)
            
            # Add file source information
            for t in transactions:
                t['Source File'] = os.path.basename(pdf_file)
            
            all_transactions.extend(transactions)
        
        if not all_transactions:
            logger.warning("No transactions found in any PDF file.")
            return None
        
        # Create DataFrame
        df = pd.DataFrame(all_transactions)
        
        # Clean up and sort data
        if 'Fecha' in df.columns:
            try:
                df['Fecha'] = pd.to_datetime(df['Fecha'], format='%d-%m-%Y')
                df = df.sort_values('Fecha')
                df['Fecha'] = df['Fecha'].dt.strftime('%d-%m-%Y')
            except Exception as e:
                logger.warning(f"Could not sort by date: {e}")
        
        # Export to Excel
        output_file = os.path.join(self.config.PDF_FOLDER, 
                                  f"transferencias_recibidas_{len(df)}_registros.xlsx")
        df.to_excel(output_file, index=False)
        
        logger.info(f"Extracted {len(df)} transactions from PDFs to {output_file}")
        return output_file
    
    def _merge_excel_files(self) -> Optional[str]:
        """Merge all Excel files in the folder."""
        return self.excel_processor.merge_excel_files(self.config.EXCEL_FOLDER)

 