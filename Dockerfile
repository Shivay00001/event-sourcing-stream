FROM node:18-alpine AS builder
WORKDIR /app
COPY package*.json ./
RUN npm ci || npm install
COPY . .
RUN npm run build --if-present

FROM node:18-alpine
WORKDIR /app
COPY --from=builder /app ./
ENV NODE_ENV=production
EXPOSE 3000
CMD ["npm", "start"]
