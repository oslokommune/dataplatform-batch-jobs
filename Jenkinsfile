@Library('k8s-jenkins-pipeline')

import no.ok.build.k8s.jenkins.pipeline.stages.*
import no.ok.build.k8s.jenkins.pipeline.stages.python.*
import no.ok.build.k8s.jenkins.pipeline.pipeline.*
import no.ok.build.k8s.jenkins.pipeline.common.*
import java.util.function.Predicate

String test = """
              make test BUILD_VENV=/tmp/virtualenv
              """

PythonConfiguration.instance.setContainerRepository("python")
PythonConfiguration.instance.setPythonVersion("3.7")

Pipeline pipeline = new Pipeline(this)
  .addStage(new ScmCheckoutStage())
  .addStage(new PythonStage(test))

pipeline.execute()