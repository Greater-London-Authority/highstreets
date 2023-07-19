pipeline {
    agent any

    parameters {
        string(defaultValue: '', description: 'Start Date (YYYY-MM-DD)', name: 'START_DATE')
        string(defaultValue: '', description: 'End Date (YYYY-MM-DD)', name: 'END_DATE')
    }

    environment {
        CONSUMER_KEY = credentials('consumer-key') // Use Jenkins credentials to securely store the consumer key
        CONSUMER_SECRET = credentials('consumer-secret') // Use Jenkins credentials to securely store the consumer secret
        PG_DATABASE = credentials('pg-database')
        PG_USER = credentials('pg-user')
        PG_PASSWORD = credentials('pg-password')
        PG_HOST = credentials('pg-host')
        PG_PORT = credentials('pg-port')
    }

    stages {
        stage('Checkout') {
            steps {
                checkout scm
            }
        }

        stage('Install Dependencies') {
            steps {
                sh 'pip install poetry'
                sh 'poetry install'
            }
        }
        
        stage('Build') {
            steps {
                script {
                    def start_date = params.START_DATE
                    def end_date = params.END_DATE

                    // Set the parameter value as an enviroment variable
                    env.START_DATE = start_date
                    env.END_DATE = end_date

                    withEnv(["CONSUMER_KEY=${CONSUMER_KEY}", "CONSUMER_SECRET=${CONSUMER_SECRET}", "PG_DATABASE=${PG_DATABASE}", "PG_USER=${PG_USER}", "PG_PASSWORD=${PG_PASSWORD}", "PG_HOST=${PG_HOST}", "PG_PORT=${PG_PORT}"]) {
                        sh 'poetry run python highstreets/hsdsprocess/bt_hex.py'
                    }
                }
            }
        }
    }
}
