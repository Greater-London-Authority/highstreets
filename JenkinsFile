pipeline {
  agent any

  environment {
    CONSUMER_KEY = credentials('consumer-key') // Use Jenkins credentials to securely store the consumer key
    CONSUMER_SECRET = credentials('consumer-secret') // Use Jenkins credentials to securely store the consumer secret
  }

  stages {
    stage('API Request') {
      steps {
        sh 'export CONSUMER_KEY=$CONSUMER_KEY'
        sh 'export CONSUMER_SECRET=$CONSUMER_SECRET'
        sh 'python api_request.py ${START_DATE} ${END_DATE}'
      }
    }

    stage('Get Data') {
      steps {
        sh 'export CONSUMER_KEY=$CONSUMER_KEY'
        sh 'export CONSUMER_SECRET=$CONSUMER_SECRET'
        sh 'python get_data.py'
      }
    }

    stage('Transform Data') {
      steps {
        sh 'export CONSUMER_KEY=$CONSUMER_KEY'
        sh 'export CONSUMER_SECRET=$CONSUMER_SECRET'
        sh 'python transform_data.py'
      }
    }
  }
}
