# Use lightweight but stable image
FROM node:20-alpine

# Set working directory
WORKDIR /app

# Copy only dependency files first (better caching)
COPY package*.json ./

# Install dependencies
RUN npm install

# Copy rest of the code
COPY . .

# Default command (override per service in compose)
CMD ["npm", "start"]