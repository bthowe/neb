FROM python

# copy all the files to the container
COPY .. .

# upgrade pip
RUN pip install --upgrade pip

# install dependencies
RUN pip install -r requirements.txt

# run the command
CMD ["python3", "tools/neb_command.py"]
