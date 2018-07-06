FROM	python:3.6

RUN	groupadd -r immp \
&&	useradd -r -g immp immp

RUN	mkdir -p /usr/src/app
WORKDIR	/usr/src/app

COPY	requirements.txt /usr/src/app/immp/
RUN	pip3 install --no-cache-dir -r requirements.txt

COPY	. /usr/src/app/

USER	immp

CMD	[ "python3", "-m", "immp", "/config.yaml" ]
