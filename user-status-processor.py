import pandas as pd
import logging
from datetime import datetime
from typing import List, Dict, Optional, Tuple
import os

class UserStatusProcessor:
    def __init__(self, log_level: str = "INFO"):
        self.setup_logging(log_level)
        self.logger = logging.getLogger(__name__)
    
    def setup_logging(self, log_level: str) -> None:
        logging.basicConfig(
            level=getattr(logging, log_level),
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
    
    def clean_string(self, value) -> str:
        """Limpia y convierte valores a string de manera segura."""
        if pd.isna(value):  # Maneja NaN y None
            return ""
        return str(value).strip()

    def clean_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Limpia el DataFrame convirtiendo valores a string donde sea necesario."""
        df_clean = df.copy()
        
        # Convertir columnas específicas a string
        string_columns = ['email', 'Work Email', 'active', 'Employment Status']
        for col in string_columns:
            if col in df_clean.columns:
                df_clean[col] = df_clean[col].apply(self.clean_string)
        
        return df_clean

    def read_csv_file(self, file_path: str) -> Optional[pd.DataFrame]:
        try:
            self.logger.info(f"Leyendo archivo: {file_path}")
            df = pd.read_csv(file_path)
            df = self.clean_dataframe(df)  # Limpiamos los datos
            self.logger.info(f"Archivo leído exitosamente. Registros: {len(df)}")
            return df
        except Exception as e:
            self.logger.error(f"Error al leer el archivo {file_path}: {str(e)}")
            return None

    def validate_data(self, ninjo_df: pd.DataFrame, output_df: pd.DataFrame, terminated_emails: set) -> Tuple[bool, str]:
        try:
            # 1. Verificar duplicados
            if output_df['email'].duplicated().any():
                return False, "El output contiene emails duplicados"

            # 2. Verificar usuarios previamente inactivos
            output_emails = set(output_df['email'].str.lower())
            ninjo_inactive = set(ninjo_df[ninjo_df['active'].str.upper() == 'FALSE']['email'].str.lower())
            
            if overlap := output_emails.intersection(ninjo_inactive):
                return False, f"El output contiene usuarios que ya estaban inactivos: {overlap}"

            # 3. Verificar usuarios terminados
            if not_terminated := output_emails.difference(terminated_emails):
                return False, f"El output contiene usuarios que no están en la lista de terminados: {not_terminated}"

            # 4. Verificar columnas requeridas
            required_columns = ['first_name', 'last_name', 'email', 'country', 'department', 
                              'branch', 'phone', 'manager', 'job_title', 'group', 'active']
            if missing := set(required_columns) - set(output_df.columns):
                return False, f"Faltan columnas requeridas en el output: {missing}"

            # 5. Verificar valores active
            if not (output_df['active'] == 'FALSE').all():
                return False, "Algunos registros no tienen 'FALSE' en el campo active"

            # 6. Verificar valores nulos
            critical_fields = ['email', 'first_name', 'last_name', 'active']
            for field in critical_fields:
                if output_df[field].isna().any():
                    return False, f"Hay valores nulos en el campo crítico: {field}"

            return True, "Validación exitosa"
        except Exception as e:
            return False, f"Error durante la validación: {str(e)}"

    def process_files(self, ninjo_file: str, terminations_file: str, output_file: str) -> bool:
        try:
            # Leer y limpiar archivos
            ninjo_df = self.read_csv_file(ninjo_file)
            terminations_df = self.read_csv_file(terminations_file)
            
            if ninjo_df is None or terminations_df is None:
                return False
            
            # Crear set de emails terminados (ya limpios por clean_dataframe)
            terminated_emails = set(
                terminations_df[
                    terminations_df['Employment Status'] == 'Terminated'
                ]['Work Email'].str.lower()
            )
            
            # Procesar datos de Ninjo (ya limpios por clean_dataframe)
            ninjo_df['email_lower'] = ninjo_df['email'].str.lower()
            ninjo_df['active_upper'] = ninjo_df['active'].str.upper()
            
            # Identificar nuevos usuarios a inactivar
            new_inactive = ninjo_df[
                (ninjo_df['active_upper'] != 'FALSE') & 
                (ninjo_df['email_lower'].isin(terminated_emails))
            ].copy()
            
            # Preparar DataFrame de salida
            output_columns = ['first_name', 'last_name', 'email', 'country', 
                            'department', 'branch', 'phone', 'manager', 
                            'job_title', 'group', 'active']
            
            output_df = new_inactive[output_columns].copy()
            output_df['active'] = 'FALSE'
            
            # Validar output
            is_valid, validation_message = self.validate_data(ninjo_df, output_df, terminated_emails)
            if not is_valid:
                self.logger.error(f"Error de validación: {validation_message}")
                return False
            
            # Logging de estadísticas
            self.logger.info("\nEstadísticas de procesamiento:")
            self.logger.info(f"- Total registros en Ninjo: {len(ninjo_df)}")
            self.logger.info(f"- Total usuarios terminados: {len(terminated_emails)}")
            self.logger.info(f"- Usuarios ya inactivos en Ninjo: {len(ninjo_df[ninjo_df['active_upper'] == 'FALSE'])}")
            self.logger.info(f"- Nuevos usuarios a inactivar: {len(output_df)}")
            
            # Generar archivo
            output_df.to_csv(output_file, index=False)
            self.logger.info(f"Archivo de salida generado exitosamente: {output_file}")
            
            # Mostrar ejemplos
            if len(output_df) > 0:
                self.logger.info("\nEjemplos de registros a inactivar:")
                for _, row in output_df.head(3).iterrows():
                    self.logger.info(f"- {row['email']} ({row['first_name']} {row['last_name']})")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error durante el procesamiento: {str(e)}")
            return False

def main():
    current_date = datetime.now().strftime("%Y%m%d")
    ninjo_file = "Ninjo-Employees-export.csv"
    terminations_file = f"{current_date}_81OP_Terminations_All_Countries_Clara.csv"
    output_file = f"{current_date}_users_to_inactivate.csv"
    
    processor = UserStatusProcessor(log_level="INFO")
    
    success = processor.process_files(
        ninjo_file=ninjo_file,
        terminations_file=terminations_file,
        output_file=output_file
    )
    
    if success:
        print(f"\nProceso completado exitosamente.")
        print(f"Archivo generado: {output_file}")
    else:
        print("\nError durante el proceso. Revise los logs para más detalles.")

if __name__ == "__main__":
    main()