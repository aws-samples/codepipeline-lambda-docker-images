FROM python:3.8-alpine AS installer
#Layer Code
COPY datalake-library /opt/
# COPY extensionssrc/requirements.txt /opt/
# RUN pip install -r /opt/requirements.txt -t /opt/extensions/lib
# WORKDIR /opt/datalake_library
# COPY extensionssrc /opt/
# COPY extensionssrc/requirements.txt /opt/
# RUN pip install -r /opt/requirements.txt -t /opt/extensions/lib
WORKDIR /opt/
# RUN chmod -R 755 *.py
RUN ls
# RUN chmod 755 *.py -R
RUN find . -name "*.py" -exec chmod 755 {} \;
# FROM scratch AS base
# # WORKDIR /opt/datalake_library
# COPY --from=installer /opt/datalake_library .
# RUN ls

FROM scratch AS base
WORKDIR /opt/
COPY --from=installer /opt/ .