# Databricks notebook source
# MAGIC %run "/Workspace/Jurídico/funcao_tratamento_fechamento/common_functions" 

# COMMAND ----------

dbutils.widgets.text("dt_driver", "", "Data driver")

# COMMAND ----------

dt_driver = dbutils.widgets.get("dt_driver")

caminho = f'/Volumes/databox/juridico_comum/arquivos/bases_drivers/ACOMPANHAMENTO_DESP_JUDICIAIS_{dt_driver}.xlsx'

base_driver_trabalhista = read_excel(caminho, "'PBI'!A1:KG1048576")

base_driver_trabalhista = adjust_column_names(base_driver_trabalhista)

base_driver_trabalhista = base_driver_trabalhista.fillna('')

base_driver_trabalhista = adjust_column_names(base_driver_trabalhista)



# COMMAND ----------

display(base_driver_trabalhista)

base_driver_trabalhista.schema

# COMMAND ----------

base_driver_trabalhista.write.format("delta") \
    .option("mergeSchema", "true") \
    .option("overwriteSchema", "true") \
    .mode("overwrite") \
    .saveAsTable(f"databox.juridico_comum.df_base_driver")

# COMMAND ----------

# import smtplib

# COMMAND ----------

# import smtplib
# from email.mime.multipart import MIMEMultipart
# from email.mime.text import MIMEText
# from email.mime.base import MIMEBase
# from email import encoders

# # Definir as configurações do servidor SMTP do Outlook
# smtp_server = "smtp.office365.com"
# smtp_port = 587

# # Credenciais do e-mail (use sua conta do Outlook)
# sender_email = "jefferson.rsilva@viavarejo.com.br"
# receiver_email = "jefferson.rsilva@viavarejo.com.br"
# password = "VIA@jeff1"  # Se necessário, utilize uma senha de aplicativo

# # Criar a mensagem de e-mail
# msg = MIMEMultipart()
# msg['From'] = sender_email
# msg['To'] = receiver_email
# msg['Subject'] = "Assunto do e-mail"

# # Corpo do e-mail
# body = "Aqui está o e-mail com o anexo."
# msg.attach(MIMEText(body, 'plain'))

# # Caminho do arquivo para anexar
# file_path = "/Volumes/databox/juridico_comum/arquivos/trabalhista/bases_indicadores_resultado/tb_inss_encerr_trab_202502_f.xlsx"

# # Adicionar o anexo
# attachment = open(file_path, "rb")
# part = MIMEBase('application', 'octet-stream')
# part.set_payload(attachment.read())
# encoders.encode_base64(part)
# part.add_header('Content-Disposition', f"attachment; filename={file_path.split('/')[-1]}")
# msg.attach(part)

# # Enviar o e-mail
# try:
#     server = smtplib.SMTP(smtp_server, smtp_port)
#     server.starttls()  # Ativar TLS
#     server.login(sender_email, password)  # Login no Outlook
#     text = msg.as_string()
#     server.sendmail(sender_email, receiver_email, text)  # Enviar o e-mail
#     print("E-mail enviado com sucesso!")
# except Exception as e:
#     print(f"Erro ao enviar e-mail: {e}")
# finally:
#     server.quit()

