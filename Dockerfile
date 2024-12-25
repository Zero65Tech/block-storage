FROM node:22-slim

WORKDIR /usr/src

RUN pwd
RUN ls -al

COPY package.json .
COPY package-lock.json .
RUN npm ci --omit=dev

COPY src src

CMD [ "npm", "start" ]