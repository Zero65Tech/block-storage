FROM node:22-slim

WORKDIR /usr/src

COPY package.json .
COPY package-lock.json .
RUN npm ci --omit=dev

COPY src src

CMD [ "npm", "start" ]