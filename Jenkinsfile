pipeline {
  agent {dockerfile {
  args "-u jenkins"}
  }
  stages {
  
    stage("prepare") {
      steps {
        script{
        sh "pipenv install --dev"
        }
      }
    }
    stage("test"){
      steps{
        sh "pipenv run pytest"
      }
    }
    stage("prepare artifact"){
      steps{
        sh "make build"
      }
    }
    
  }
}
