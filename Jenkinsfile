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
        sh 'pipenv run pytest -v --html=testreport.html'
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
          publishHTML([allowMissing: false, alwaysLinkToLastBuild: false, keepAll: false, reportDir: 'htmlreports/HTML_20Report', reportFiles: 'testreport.html', reportName: 'HTML Report', reportTitles: '', useWrapperFileDirectly: true])
            }

      }
}
