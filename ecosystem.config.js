module.exports = {
    apps: [
        {
            name: 'firehose',
            script: 'dist/index.js',
            args: '',
            watch: false,
            env: {
                NODE_ENV: 'production',
            },
            min_uptime: 120000,
            max_restarts: 50,
            restart_delay: 4000
        },
    ]
}