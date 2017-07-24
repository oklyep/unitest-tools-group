FROM python:3.5

RUN pip3 install --upgrade pip \
 docker-py \
 tornado

ENV TZ=Asia/Yekaterinburg

COPY . /usr/local/test_tools_group

EXPOSE 8888

CMD ["python3", "/usr/local/test_tools_group/main.py"]
