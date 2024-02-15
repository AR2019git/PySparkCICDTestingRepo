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
        echo 'This is the test stage - Test'
        sh 'pipenv run pytest --html=testreport.html'
      }
    }
    stage("prepare artifact"){
      steps{
      echo 'This is the make part but commented'
        }
    }
    
  }
  post {
            always {
                  publishHTML([allowMissing: false, alwaysLinkToLastBuild: true, keepAll: false, reportDir: '', reportFiles: 'testreport.html', reportName: 'Testing Result report', reportTitles: 'Testing Result report', useWrapperFileDirectly: true])
            }
      }
}
