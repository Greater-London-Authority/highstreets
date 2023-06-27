pipeline {
    agent any

    parameters {
        string(defaultValue: '', description: 'Start Date (YYYY-MM-DD)', name: 'START_DATE')
        string(defaultValue: '', description: 'End Date (YYYY-MM-DD)', name: 'END_DATE')
    }

    environment {
        CONSUMER_KEY = credentials('consumer-key') // Use Jenkins credentials to securely store the consumer key
        CONSUMER_SECRET = credentials('consumer-secret') // Use Jenkins credentials to securely store the consumer secret
    }

    stages {
        stage('Checkout') {
            steps {
                checkout scm
            }
        }

        stage('Install Poetry') {
            steps {
                sh 'pip install poetry'
            }
        }

        stage('Install Dependencies') {
            steps {
                sh 'poetry install'
            }
        }

        stage('Data Loading') {
            steps {
                script {
                    def start_date = params.START_DATE
                    def end_date = params.END_DATE

                    withEnv(["CONSUMER_KEY=${CONSUMER_KEY}", "CONSUMER_SECRET=${CONSUMER_SECRET}"]) {
                        sh 'cd hsdhsdsprocess'
                        sh 'bt_hex.py'
                    }
                }
            }
        }
    }
}
