FROM node:18-alpine
WORKDIR /usr/src/app
COPY package.json ./
RUN npm install
COPY index.js dashboard.html ./
CMD ["node", "index.js"]