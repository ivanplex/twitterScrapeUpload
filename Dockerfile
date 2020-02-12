FROM python:3.7-slim
Add /crawler /
RUN pip install -r requirements.txt
CMD [ "python", "./execute.py"]