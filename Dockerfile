FROM node:22-slim

WORKDIR /usr/src/app

COPY package.json .
COPY package-lock.json .
RUN npm ci

COPY src src

CMD [ "npm", "start" ]