FROM python:3.7-slim
ENV PYTHONUNBUFFERED 1
Add /crawler /
RUN pip install -r requirements.txt
CMD [ "python", "./execute.py"]