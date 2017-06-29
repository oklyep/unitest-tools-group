FROM python:3.4

RUN pip3 install --upgrade pip \
 docker-py==1.10 \
 tornado==4.5

ENV TZ=Asia/Yekaterinburg

COPY . /usr/local/test_tools_group

EXPOSE 8888

CMD ["python3", "/usr/local/test_tools_group/main.py"]
