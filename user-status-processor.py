import pandas as pd
import logging
from datetime import datetime
from typing import List, Dict, Optional, Tuple
import os

class UserStatusProcessor:
    def __init__(self, log_level: str = "INFO"):
        """
        Inicializa el procesador de status de usuarios.
        
        Args:
            log_level: Nivel de logging (default: "INFO")
        """
        # Configurar logging
        self.setup_logging(log_level)
        self.logger = logging.getLogger(__name__)
    
    def setup_logging(self, log_level: str) -> None:
        """Configura el sistema de logging."""
        logging.basicConfig(
            level=getattr(logging, log_level),
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
    
    def read_csv_file(self, file_path: str) -> Optional[pd.DataFrame]:
        """
        Lee un archivo CSV y lo convierte en DataFrame.
        
        Args:
            file_path: Ruta al archivo CSV
            
        Returns:
            DataFrame con los datos del CSV o None si hay error
        """
        try:
            self.logger.info(f"Leyendo archivo: {file_path}")
            df = pd.read_csv(file_path)
            self.logger.info(f"Archivo leído exitosamente. Registros: {len(df)}")
            return df
        except Exception as e:
            self.logger.error(f"Error al leer el archivo {file_path}: {str(e)}")
            return None

    def validate_data(self, 
                     ninjo_df: pd.DataFrame, 
                     output_df: pd.DataFrame, 
                     terminated_emails: set) -> Tuple[bool, str]:
        """
        Valida que el output contenga solo las nuevas bajas.
        
        Args:
            ninjo_df: DataFrame original de Ninjo
            output_df: DataFrame de salida
            terminated_emails: Set de emails terminados
            
        Returns:
            Tuple[bool, str]: (Es válido, Mensaje de error)
        """
        try:
            # 1. Verificar que no haya duplicados en el output
            if output_df['email'].duplicated().any():
                return False, "El output contiene emails duplicados"

            # 2. Verificar que todos los registros del output estaban activos en Ninjo
            output_emails = set(output_df['email'].str.lower())
            ninjo_inactive = set(ninjo_df[ninjo_df['active'].str.upper() == 'FALSE']['email'].str.lower())
            if overlap := output_emails.intersection(ninjo_inactive):
                return False, f"El output contiene usuarios que ya estaban inactivos: {overlap}"

            # 3. Verificar que todos los registros del output están en la lista de terminados
            if not_terminated := output_emails.difference(terminated_emails):
                return False, f"El output contiene usuarios que no están en la lista de terminados: {not_terminated}"

            # 4. Verificar que todos los campos requeridos están presentes
            required_columns = ['first_name', 'last_name', 'email', 'country', 'department', 
                              'branch', 'phone', 'manager', 'job_title', 'group', 'active']
            if missing := set(required_columns) - set(output_df.columns):
                return False, f"Faltan columnas requeridas en el output: {missing}"

            # 5. Verificar que todos los registros tienen 'FALSE' en active
            if not (output_df['active'] == 'FALSE').all():
                return False, "Algunos registros no tienen 'FALSE' en el campo active"

            # 6. Verificar que no hay valores nulos en campos críticos
            critical_fields = ['email', 'first_name', 'last_name', 'active']
            for field in critical_fields:
                if output_df[field].isnull().any():
                    return False, f"Hay valores nulos en el campo crítico: {field}"

            return True, "Validación exitosa"

        except Exception as e:
            return False, f"Error durante la validación: {str(e)}"

    def process_files(self, 
                     ninjo_file: str, 
                     terminations_file: str, 
                     output_file: str) -> bool:
        """
        Procesa los archivos de entrada y genera el archivo de salida.
        
        Args:
            ninjo_file: Ruta al archivo de Ninjo
            terminations_file: Ruta al archivo de terminaciones
            output_file: Ruta donde se guardará el archivo de salida
            
        Returns:
            bool: True si el proceso fue exitoso, False en caso contrario
        """
        try:
            # Leer archivos
            ninjo_df = self.read_csv_file(ninjo_file)
            terminations_df = self.read_csv_file(terminations_file)
            
            if ninjo_df is None or terminations_df is None:
                return False
            
            # Obtener emails de usuarios terminados
            terminated_emails = set(
                terminations_df[
                    terminations_df['Employment Status'] == 'Terminated'
                ]['Work Email'].str.lower().dropna()
            )
            
            # Normalizar datos de Ninjo
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
            
            # Validar el output
            is_valid, validation_message = self.validate_data(
                ninjo_df, output_df, terminated_emails
            )
            
            if not is_valid:
                self.logger.error(f"Error de validación: {validation_message}")
                return False
            
            # Registrar estadísticas antes de guardar
            self.logger.info(f"Estadísticas de procesamiento:")
            self.logger.info(f"- Total registros en Ninjo: {len(ninjo_df)}")
            self.logger.info(f"- Total usuarios terminados: {len(terminated_emails)}")
            self.logger.info(f"- Usuarios ya inactivos en Ninjo: {len(ninjo_df[ninjo_df['active_upper'] == 'FALSE'])}")
            self.logger.info(f"- Nuevos usuarios a inactivar: {len(output_df)}")
            
            # Generar archivo de salida
            output_df.to_csv(output_file, index=False)
            self.logger.info(f"Archivo de salida generado exitosamente: {output_file}")
            
            # Mostrar ejemplos de registros procesados
            if len(output_df) > 0:
                self.logger.info("\nEjemplos de registros a inactivar:")
                for _, row in output_df.head(3).iterrows():
                    self.logger.info(f"- {row['email']} ({row['first_name']} {row['last_name']})")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error durante el procesamiento: {str(e)}")
            return False

def main():
    # Configurar rutas de archivos
    current_date = datetime.now().strftime("%Y%m%d")
    ninjo_file = "NinjoEmployees export.csv"
    terminations_file = f"{current_date}_81OP_Terminations_All_Countries_Clara 1.csv"
    output_file = f"{current_date}_users_to_inactivate.csv"
    
    # Crear instancia del procesador
    processor = UserStatusProcessor(log_level="INFO")
    
    # Procesar archivos
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
