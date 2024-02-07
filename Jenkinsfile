pipeline {
    agent any

    stages {
        stage('Build') {
            steps {
                script {
                    def dockerPath = tool 'Docker'
                    env.PATH = "${dockerPath}/bin:${env.PATH}"
                    sh 'docker --version'
                }
            }
        }
    }
}
