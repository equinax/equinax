# Frontend Dockerfile
FROM node:20-alpine

WORKDIR /app

# Install pnpm
RUN npm install -g pnpm

# Copy package files
COPY package.json pnpm-lock.yaml* ./

# Install dependencies
RUN pnpm install --frozen-lockfile || pnpm install

# Copy source code
COPY . .

# Expose port
EXPOSE 3000

# Development command (can be overridden)
CMD ["pnpm", "dev", "--host", "0.0.0.0", "--port", "3000"]
