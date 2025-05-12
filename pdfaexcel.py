import os
import re
import pandas as pd
import pdfplumber
import glob
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import numpy as np
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
PERSONAS_EXCLUIDAS = os.getenv("PERSONAS_EXCLUIDAS")
KEY_SHEET = os.getenv("KEY_SHEET") 
pdf_folder = os.getenv("PDF_FOLDER") 
    # Get the path to the folder with Excel files to merge
excel_folder = os.getenv("EXCEL_FOLDER") 
# Get output folder
output_folder = os.getenv("OUTPUT_FOLDER") 

    

def extract_transfers_from_pdf(pdf_path):
    """Extract 'Transferencia recibida' transactions from a PDF file with enhanced pattern matching."""
    transactions = []
    
 
    with pdfplumber.open(pdf_path) as pdf:
            # Process each page individually to handle page breaks better
            for page_num, page in enumerate(pdf.pages, 1):
                text = page.extract_text()
            
                # Split text into lines for processing
                lines = text.split('\n')
        
                for i, line in enumerate(lines):
                                              
                    # Check for 'Transferencia recibida' in the line
                    if 'Transferencia' in line:
                           
                        #pattern1 = r'(\d{2}-\d{2}-\d{4})\s+(Transferencia\s+[^$]+)\s+(\d{9,})\s+\$\s*(-?[\d.,]+)'
                        pattern1 =  r'(\d{2}-\d{2}-\d{4})\s+(Transferencia\s+.+?)\s+(\d{9,})\s+\$\s*(-?[\d.,]+)(?:\s+\$\s*[\d.,]+)?'
                                    
                        match1 = re.search(pattern1, line)
                        
                        if match1:
                            fecha, description, op_id, value_str = match1.groups()
                            value = value_str.replace('.', '').replace(',', '.')
                             
                            transactions.append({
                                    'Fecha': fecha,                   # ahora usamos la fecha recién extraída
                                    'Descripción': description,
                                    'ID de la operación': op_id,
                                    'Valor': float(value)
                                  })
                            
                            continue
                            
                        # Strategy 2: Use current_date and extract parts
                        # Extract description 
                        desc_match = re.search(r'(\d{2}-\d{2}-\d{4})\s+(Transferencia\s+.+?)(?:\s{2,}|\d{9,}|$)', line)
                        
                        if desc_match:
                                fecha,description = desc_match.groups() 
                                
                                # Extract operation ID - look for 9+ digit number
                                id_match = re.search(r'(\d{9,})', line)
                                op_id = id_match.group(1) if id_match else "Unknown"
                                 
                                # Extract value - look for $ followed by numbers
                                value_match = re.search(r'\$\s*(-?[\d.,]+)', line)
                                value_str = value_match.group(1).replace('.', '').replace(',', '.')
                                if value_match :                                  
                                    transactions.append({
                                        'Fecha': fecha,
                                        'Descripción': description,
                                        'ID de la operación': op_id,
                                        'Valor': float(value_str)
                                    })
                                
                                    continue
                        
                        
                       # Strategy 3: multilínea de 3 líneas con extracción de fecha del ID-line
                        desc_match = re.search(r'(Transferencia\s+.+?)$', line)
                        if desc_match and i+1 < len(lines):
                            # 1) Capturamos la descripción en la línea i
                            description = desc_match.group(1).strip()

                            # 2) Analizamos la línea i+1 para ID, valor y fecha
                            next_line = lines[i+1]
                            parts_match = re.search(r'(\d{2}-\d{2}-\d{4})\s+(\d{9,})\s+\$\s*(-?[\d.,]+)', next_line)
                            if parts_match:
                                # Extraemos fecha, ID y valor
                                fecha, op_id, value_str = parts_match.groups()
                                value = float(value_str.replace('.', '').replace(',', '.'))
                                
                                # 3) Miramos la tercera línea para ver si sigue la descripción
                                if i+2 < len(lines):
                                    third_line = lines[i+2].strip()
                                    # Si no comienza con fecha ni contiene un ID, lo añadimos
                                    if (not re.match(r'^\d{2}-\d{2}-\d{4}', third_line)
                                        and not re.search(r'\d{9,}', third_line)):
                                        description += ' ' + third_line
                                transactions.append({
                                        'Fecha': fecha,                   # ahora usamos la fecha recién extraída
                                        'Descripción': description,
                                        'ID de la operación': op_id,
                                        'Valor': value
                                    })
                    # Check for 'Pago' in the line
                    keywords = ['Balanceados', 'Pet', 'Poppi', 'VETERINARIA', 'CABIFY']

                    if 'Pago ' in line and any(kw in line for kw in keywords):      


                        #pattern1 = r'(\d{2}-\d{2}-\d{4})\s+(Transferencia\s+[^$]+)\s+(\d{9,})\s+\$\s*(-?[\d.,]+)'
                        pattern1 =  r'(\d{2}-\d{2}-\d{4})\s+(Pago\s+.+?)\s+(\d{9,})\s+\$\s*(-?[\d.,]+)(?:\s+\$\s*[\d.,]+)?'
                                    
                        match1 = re.search(pattern1, line)
                        
                        if match1:
                            fecha, description, op_id, value_str = match1.groups()
                            value = value_str.replace('.', '').replace(',', '.')
                             
                            transactions.append({
                                    'Fecha': fecha,                   # ahora usamos la fecha recién extraída
                                    'Descripción': description,
                                    'ID de la operación': op_id,
                                    'Valor': float(value)
                                  })
                            
                            continue

                            
   
    
    return transactions




def merge_excel_files(excel_folder, output_name="transaccion_unificadas.xlsx"):
    """
    Merge all Excel files in the specified folder into a single Excel file.
    Ensures ID fields are preserved as text.
    
    Args:
        excel_folder (str): Path to the folder containing Excel files
        output_name (str): Name of the output merged Excel file
    
    Returns:
        str: Path to the merged Excel file
    """
    print(f"Merging Excel files from {excel_folder}...")
    
    # Get all Excel files in the folder
    excel_files = glob.glob(os.path.join(excel_folder, "*.xlsx"))
    excel_files.extend(glob.glob(os.path.join(excel_folder, "*.xls")))
    
    if not excel_files:
        print("No Excel files found in the specified folder.")
        return None
    
    # Merge all Excel files
    combined_df = pd.DataFrame()
    
    for excel_file in excel_files:
        try:
            # Define data types for specific columns - particularly ID columns
            dtypes = {'ID DE OPERACIÓN EN MERCADO PAGO': str}
            
            # Try to read with explicit dtype first
            try:
                df = pd.read_excel(excel_file, dtype=dtypes)
            except:
                # If that fails, read normally then convert after
                df = pd.read_excel(excel_file)
                
                # Look for ID columns and convert them to string
                for col in df.columns:
                    if 'ID' in col.upper() and ('OPERACI' in col.upper() or 'MERCADO' in col.upper()):
                        df[col] = df[col].astype(str)
            
            
            combined_df = pd.concat([combined_df, df], ignore_index=True)
        except Exception as e:
            print(f"    Error reading {excel_file}: {e}")
    
    if combined_df.empty:
        print("No data found in Excel files.")
        return None
    
    # Ensure ID columns are strings in combined dataframe
    for col in combined_df.columns:
        if 'ID' in col.upper() and ('OPERACI' in col.upper() or 'MERCADO' in col.upper()):
            combined_df[col] = combined_df[col].astype(str)
            print(f"  Ensuring column '{col}' is stored as text")
    
    # Save the combined DataFrame to a new Excel file with string preservation
    output_path = os.path.join(excel_folder, output_name)
    # 2) Si ya había un Excel con ese nombre, lo borro
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
    
    print(f"Merged {len(excel_files)} Excel files with a total of {len(combined_df)} rows")
    print(f"Output saved to {output_path} with ID columns preserved as text")
    
    return output_path

 

def create_final_excel(pdf_extracts_path, unified_excel_path, output_folder):
    # ——— Leer orígenes ———
    pdf_data     = pd.read_excel(pdf_extracts_path)
    unified_data = pd.read_excel(unified_excel_path)
    pdf_data = pdf_data[ ~(pdf_data['Descripción'].str.contains('Transferencia cancelada', case=False, na=False))
]

    # ——— Limpiar DESCRIPCIÓN ———
    def extraer_descripcion(d):
        if not isinstance(d, str):
            return d
        for prefijo in ("Transferencia recibida", "Transferencia enviada","Transferencia","Pago"):
            if d.startswith(prefijo):
                # Corto justo el prefijo y devuelvo lo que sigue
                return d[len(prefijo):].strip()
        return d

    pdf_data['Descripción'] = pdf_data['Descripción'].apply(extraer_descripcion)
    # ——— Normalizar tipos ———
    pdf_data['ID de la operación'] = pdf_data['ID de la operación'].astype(str)
    unified_data["ID DE OPERACIÓN EN MERCADO PAGO"] = unified_data["ID DE OPERACIÓN EN MERCADO PAGO"].astype(str)

    # ——— Mapear columnas de unified_data ———
    cols_map = {}
    for key in [
        'ID DE OPERACIÓN EN MERCADO PAGO',
        'FECHA DE ORIGEN',
        'MEDIO DE PAGO',
        'TIPO DE IDENTIFICACIÓN DEL PAGADOR',
        'NÚMERO DE IDENTIFICACIÓN DEL PAGADOR',
        'PAGADOR',
        'DETALLE DE LA VENTA'
    ]:
        for col in unified_data.columns:
            if col.upper() == key:
                cols_map[key] = col
                break

    # ——— Construir filas finales ———
    final_rows = []
    for _, pdf_row in pdf_data.iterrows():
        pid = pdf_row['ID de la operación']
        uni = unified_data[unified_data["ID DE OPERACIÓN EN MERCADO PAGO"] == pid]
        uni = uni.iloc[0] if not uni.empty else {}

        # FECHA: preferir FECHA DE ORIGEN, si no, usar Fecha del PDF
        fecha = uni.get(cols_map.get('FECHA DE ORIGEN'))
        if pd.isna(fecha):
            fecha = pdf_row.get('Fecha')
                # Leer el valor crudo
        fecha_raw = uni.get(cols_map.get('FECHA DE ORIGEN'))
        if pd.isna(fecha_raw):
            fecha_raw = pdf_row.get('Fecha')

        # 1) Convertir a Timestamp (intenta inferir formatos ISO o dd/mm/YYYY)
        fecha_ts = pd.to_datetime(fecha_raw, errors="coerce")

        # 3) Formatear siempre como "DD/MM/YYYY HH:MM:SS"
        fecha = fecha_ts.strftime("%d/%m/%Y %H:%M:%S")
        medio   = uni.get(cols_map.get('MEDIO DE PAGO'), "")
        tipo_id = uni.get(cols_map.get('TIPO DE IDENTIFICACIÓN DEL PAGADOR'), "")
        num_id  = uni.get(cols_map.get('NÚMERO DE IDENTIFICACIÓN DEL PAGADOR'), "")
        pagador = uni.get(cols_map.get('PAGADOR'), "")
        detalle = uni.get(cols_map.get('DETALLE DE LA VENTA'), "")

        final_rows.append({
            'ID DE LA OPERACION': pid,
            'FECHA':             fecha,
            'MEDIO DE PAGO':     medio,
            'TIPO DE IDENTIFICACIÓN DEL PAGADOR': tipo_id,
            'NÚMERO DE IDENTIFICACIÓN DEL PAGADOR': num_id,
            'PAGADOR':           pagador,
            'DETALLE DE LA VENTA': detalle,
            'Descripción':       pdf_row['Descripción'],
            'VALOR':             pdf_row.get('Valor')   # ← agregamos aquí el campo Valor del PDF
        })

    df = pd.DataFrame(final_rows)

    # ——— Columna DONANTE ———
    df['DONANTE'] = df['Descripción'].where(
        df['Descripción'].str.strip() != "",
        df['PAGADOR']
    )

    # ——— Excluir nombres indeseados ———
    excluir = PERSONAS_EXCLUIDAS 
    df = df[~df['DONANTE'].isin(excluir)]

    # ——— Seleccionar columnas finales y volcar a Excel ———
    df_final = df[[
        'ID DE LA OPERACION',
        'FECHA',
        'MEDIO DE PAGO',
        'TIPO DE IDENTIFICACIÓN DEL PAGADOR',
        'NÚMERO DE IDENTIFICACIÓN DEL PAGADOR',
        'DONANTE',
        'DETALLE DE LA VENTA',
        'VALOR'   # ← incluimos Valor en el Excel final
    ]]

 
    df_final = df_final.replace([np.inf, -np.inf], np.nan)  # Convertir infinitos a NaN
    df_final = df_final.fillna('')  # Convertir NaN a strings vacíos

    df_positivos = df_final[df_final['VALOR'] >= 0].copy()
    df_negativos = df_final[df_final['VALOR'] < 0].copy()
    df_negativos['VALOR'] = df_negativos['VALOR'] * -1
 
        # Lista exacta de gastos que quieres conservar
    gastos_a_incluir = ['VETERINA','LINARES','MASCOTAS', 'PET','POPPI','WALTER EDUARDO PEREZ', 'CABIFY', 'BALANCEADOS']
    pattern = '|'.join(map(re.escape, gastos_a_incluir))

    # 2) Filtramos: pasamos todo a mayúsculas y buscamos el patrón
    df = df_negativos[df_negativos['DONANTE'].str.upper().str.contains(pattern, regex=True, na=False)].copy() 
    df['FECHA'] = pd.to_datetime(df['FECHA'],format="%d/%m/%Y %H:%M:%S", errors='coerce')
        # 2) Crear máscaras
    mask_cabify  = df['DONANTE'].str.upper().str.contains(r'\bCABIFY\b', na=False)
    mask_weekend = df['FECHA'].dt.weekday.isin([5, 6])  # sábado=5, domingo=6

    # 3) Filtrar: quitamos solo los Cabify que NO sean fin de semana
    df = df[~(mask_cabify & ~mask_weekend)].copy()

    # 1) Renombrar columnas base
    df = df.rename(columns={
        'DONANTE':                  'Nombre de Proveedor',
        'DETALLE DE LA VENTA':      'DETALLE',
        'VALOR':                    'MONTO',
        'ID DE LA OPERACION':       'Observacion'
    })


    # 2) Calcular ‘Tipo de gasto’ según patrones en el Nombre de Proveedor
    #    Si aparece alguno de estos términos → “Veterinaria”, si no → “Alimentos”

    conds = [
    df['Nombre de Proveedor'].str.upper().str.contains(r'\b(?:WALTER EDUARDO PEREZ|VETERINAR|LINARES, MARCELO)\b'),
    df['Nombre de Proveedor'].str.upper().str.contains(r'\bCABIFY\b')    ]
    df['Tipo de gasto'] = np.select(conds, ['Veterinaria','Transporte'], default='Alimentos')

    # 3) Añadir columnas fijas o vacías
    df['MASCOTA']                          = ''
    df['RESPONSABLE']                      = 'LARA GERALDINE'
    df['FORMA DE PAGO']                    = 'MERCADOPAGO'
    df['Tiempo Total (con traslados)']     = ''
    df['foto']                             = ''

    # 4) Seleccionar y ordenar las columnas como querés
    df_negativos_final = df[[
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
    # 1) subir positivos
    #agregar_dataframe_a_sheets_directo(df_positivos)

    # 2) guardar negativos localmente
    negativos_path = os.path.join(output_folder, "transacciones_negativas.xlsx")
    df_negativos_final.to_excel(negativos_path, index=False)

    print(f"{len(df_positivos)} transacciones positivas subidas al Drive.")
    print(f"{len(df_negativos_final)} transacciones negativas guardadas en {negativos_path}")

    
    return negativos_path
 


def agregar_dataframe_a_sheets_directo(df):
    """
    Agrega un DataFrame directamente a Google Sheets sin archivos temporales
    
    Args:
        df: DataFrame con los datos a agregar
        spreadsheet_id: ID de la hoja de cálculo en Google Drive
        worksheet_name: Nombre de la hoja donde agregar los datos
    """

    
    # Definir permisos
    scope = [
        'https://spreadsheets.google.com/feeds',
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive'
    ]
   

    credentials = ServiceAccountCredentials.from_json_keyfile_name("credenciales.json", scopes=scope)
    
    # Para OAuth (similar a lo que ya tenías):
    # credentials = tu_función_para_obtener_oauth_credentials()
    
    gc = gspread.authorize(credentials)
    
    # Abrir la hoja de cálculo por ID

    spreadsheet = gc.open_by_key(KEY_SHEET)
    
   # Intentar abrir la hoja existente
    worksheet = spreadsheet.worksheet("Transaccion donaciones")
        
        # Obtener valores existentes
    existing_data = worksheet.get_all_values()
        
    if len(existing_data) > 0:
            # Si hay datos, añadir el DataFrame al final
            valores = df.values.tolist()
            # Añadir encabezados solo si la hoja está vacía
            if len(existing_data) == 1:  # Solo tiene encabezados
                worksheet.append_rows(valores)
            else:
                # Verificar si los encabezados coinciden
                headers_exist = existing_data[0]
                if headers_exist == df.columns.tolist():
                    worksheet.append_rows(valores)
                else:
                    # Si los encabezados no coinciden, podríamos manejar esto de diferentes maneras
                    # Por ejemplo, reordenar el DataFrame según los encabezados existentes
                    print("Advertencia: Los encabezados no coinciden exactamente")
                    worksheet.append_rows(valores)
    else:
            # Si la hoja está completamente vacía, incluir encabezados
            headers = df.columns.tolist()
            all_values = [headers] + df.values.tolist()
            worksheet.append_rows(all_values)
    print(f"DataFrame agregado exitosamente a Google Sheets '{spreadsheet.title}'")

def main():
   
     
    # Get all PDF files in the folder
    pdf_files = glob.glob(os.path.join(pdf_folder, "*.pdf"))
    
    if not pdf_files:
        print("No PDF files found in the specified folder.")
        return
    
    # Extract transactions from all PDFs
    all_transactions = []
    
    for pdf_file in pdf_files:
        
        transactions = extract_transfers_from_pdf(pdf_file)
        # Debug information
                
        # Add file source information
        for t in transactions:
            t['Source File'] = os.path.basename(pdf_file)
        
        all_transactions.extend(transactions)
    
    if not all_transactions:
        print("No 'Transferencia recibida' transactions found in any PDF file.")
        return
    
    # Create DataFrame
    df = pd.DataFrame(all_transactions)
    
    # Clean up data
    if 'Fecha' in df.columns:
        # Convert to datetime for sorting
        try:
            df['Fecha'] = pd.to_datetime(df['Fecha'], format='%d-%m-%Y')
            df = df.sort_values('Fecha')
            df['Fecha'] = df['Fecha'].dt.strftime('%d-%m-%Y')
        except Exception as e:
            print(f"Warning: Could not sort by date - {e}")
    
    # Export to Excel
    output_pdftoexcelfile = os.path.join(pdf_folder, f"transferencias_recibidas_{len(df)}_registros.xlsx")
    df.to_excel(output_pdftoexcelfile , index=False)
    
    print(f"\nExtraction complete! Total of {len(df)} incoming transfers found and saved to {output_pdftoexcelfile}")
 

    print("Excel Merger and Data Combiner\n")
    

    # Merge Excel files
    unified_excel_path = merge_excel_files(excel_folder)
    
    if not unified_excel_path:
        print("Failed to create unified Excel file. Exiting.")
        return
    
    if not os.path.exists(output_folder):
        try:
            os.makedirs(output_folder)
            print(f"Created output folder: {output_folder}")
        except Exception as e:
            print(f"Error creating output folder: {e}")
            return
    
    # Create final Excel
    create_final_excel(output_pdftoexcelfile , unified_excel_path, output_folder)
    print("\nProcess completed successfully!")

if __name__ == "__main__":
    main()