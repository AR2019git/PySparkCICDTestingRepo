pipeline {
    agent any

    stages {
        stage('Install and Build') {
            steps {
                script {
                    sh 'apt-get update && apt-get install -y docker.io'
                    sh 'docker --version'
                    // Continue with your Docker-related commands
                }
            }
        }
    }
}
