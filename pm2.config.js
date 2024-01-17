module.exports = {
  apps: [
    {
      name: 'RABBIT-POS',
      script: 'dist/index.js',
      args: 'start',
      autorestart: true,
      max_memory_restart: '100M',
    },
  ],
};
