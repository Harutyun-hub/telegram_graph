FROM node:20-alpine AS build

WORKDIR /app

COPY package*.json ./
RUN npm ci

COPY . .
RUN npm run build

FROM caddy:2.8-alpine

WORKDIR /app

COPY Caddyfile /etc/caddy/Caddyfile
COPY --from=build /app/dist /app/dist

EXPOSE 80
