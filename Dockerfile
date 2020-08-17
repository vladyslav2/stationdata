FROM gcr.io/dataflow-templates-base/python3-template-launcher-base

ARG WORKDIR=/dataflow/template
RUN mkdir -p ${WORKDIR}
RUN python -m pip install --upgrade pip
WORKDIR ${WORKDIR}

COPY ../stationdata stationdata

ENV FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE="${WORKDIR}/stationdata/requirements.txt"
ENV FLEX_TEMPLATE_PYTHON_PY_FILE="${WORKDIR}/stationdata/main.py"
ENV FLEX_TEMPLATES_TAIL_CMD_TIMEOUT_IN_SECS=5
ENV FLEX_TEMPLATES_NUM_LOG_LINES=100
ENV PYTHONPATH "${PYTHONPATH}:${WORKDIR}/stationdata"

RUN pip install stationdata/
RUN pip install -U -r ./requirements.txt
