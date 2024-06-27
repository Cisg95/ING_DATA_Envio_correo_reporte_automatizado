import os
import pandas as pd
from pandas_gbq import read_gbq
from pandas_gbq import to_gbq
import base64
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail, Attachment, FileContent, FileName, FileType, Disposition


# Correo que envia la informacion
EMAIL_FROM = 'Equipo_do_not_reply@fpay.cl'
API_SENDGRID = os.getenv("api_sendgrid") 

# Tema de reporteria
SUBJECT = 'Transacciones Reversadas F.com Peru ultimas 24 horas (14 a 14)'

# Crear lista de correos remitentes
LISTA_CORREOS = ['xxx@xxx.cl','xxx@xxx.cl','xxx@xxx.cl']

# Nombre y extension del archivo
XLSX_FILE = 'transacciones.xlsx'

# Variables del proyecto GCP
PROYECTO_BT = 'xxxx'
PROYECTO_NRT = 'yyy'
TABLA_BT = 'zzz'
TABLA_NRT = 'vvv'


query = f"""
    SELECT 
      col1
      ,col2
      ,col3
    FROM `{PROYECTO_BT}.{TABLA_BT}` t1
    left join `fif-pay-pe-datalake-qa.acc_pay_pe_shared_tables.cross_operation_codes` t2
      on t1.operation_code = t2.operation_code
    WHERE CREATE_DATE >= CURRENT_DATE() -1
    AND extract(date from create_time_local) = CURRENT_DATE('America/Santiago') -1
    AND criteria...


    UNION ALL

    SELECT 
      col1
      ,col2
      ,col3
    FROM `{PROYECTO_NRT}.{TABLA_NRT}` t1
    left join `fif-pay-pe-datalake-qa.acc_pay_pe_shared_tables.cross_operation_codes` t2
      on t1.operation_code = t2.operation_code
    WHERE CREATE_DATE >= CURRENT_DATE() -1
    AND extract(date from create_time_local) = CURRENT_DATE('America/Santiago') -1
    AND criteria...
    """

# Consultar GCP
QUERY_RESULT = read_gbq(query,project_id=PROYECTO_BT,progress_bar_type= None)
QUERY_RESULT['xxxx'] = QUERY_RESULT['xxxx'].dt.tz_localize(None)

# Generación de archivo CSV
QUERY_RESULT.to_excel(XLSX_FILE, index=False)

# Configuración del correo electrónico
message = Mail(
    from_email=EMAIL_FROM,
    to_emails=LISTA_CORREOS,
    subject=SUBJECT,
    plain_text_content='Hola equipo,\n\nExcelente tarde.\nFavor encontrar adjunto.\nCualquier consulta contactar con Equipo de gestión yyyy.\n\nSaludos.'
)

# Adjuntar el archivo CSV al correo electrónico
with open(XLSX_FILE, 'rb') as f:
    data = f.read()
    f.close()

encoded_file = base64.b64encode(data).decode()
attachment = Attachment(
    FileContent(encoded_file),
    FileName(XLSX_FILE),
    FileType('application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'),
    Disposition('attachment')
)

message.attachment = attachment

# Enviar el correo electrónico utilizando la API de SendGrid
try:
    sg = SendGridAPIClient(API_SENDGRID)
    response = sg.send(message)
    print('Proceso corrio de manera exitosa')
except Exception as e:
    print('Proceso falló al enviar el correo')
