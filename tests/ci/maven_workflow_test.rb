# Run: ruby tests/ci/maven_workflow_test.rb (no Maven/network required).
require 'minitest/autorun'
require 'yaml'
require 'open3'
require 'tmpdir'
require 'fileutils'

class MavenWorkflowTest < Minitest::Test
  ROOT = File.expand_path('../..', __dir__)

  def test_image_bootstrap_uses_harbor_and_fails_before_building
    source = File.read(File.join(ROOT, 'build/build.sh'))
    block = source.split('  binfmt_image=', 2).last.split('  info ">> registry cache:', 2).first
    refute_nil block
    assert_includes block, 'harbor.internal.tapdata.io/tapdata/binfmt'
    assert_includes block, 'harbor.internal.tapdata.io/tapdata/buildkit'
    assert_includes block, '--driver-opt "image=$buildkit_image"'
    # Execute the real bootstrap block with mocked commands; verify fail-fast,
    # retry count, and no builder initialization after a QEMU failure.
    { 'pull' => 3, 'run' => 2, 'inspect' => 2 }.each do |failure, expected_pulls|
      prefix = <<~SH
        info() { :; }
        sleep() { :; }
        timeout() { shift 2; "$@"; }
        docker() {
          echo "$*"
          case "$1" in
            pull) [[ "$FAILURE" != pull ]];;
            run) [[ "$FAILURE" != run ]];;
            buildx) [[ "$2" != inspect || "$FAILURE" != inspect ]];;
          esac
        }
        builder_name=test-builder
        bootstrap() {
      SH
      output, error, status = Open3.capture3({'FAILURE' => failure}, 'bash', '-c', prefix + '  binfmt_image=' + block + "\n}\nbootstrap\n")
      refute status.success?, "#{failure}: #{output} #{error}"
      assert_equal expected_pulls, output.lines.count { |line| line.start_with?('pull ') }
      refute_includes output, 'buildx create' if failure == 'run'
    end
  end

  def workflow(name)
    YAML.load_file(File.join(ROOT, '.github/workflows', name))
  end

  def test_cache_seed_preserves_private_repository_and_configuration
    count = 0
    %w[mr-ci.yaml build.yml].each do |name|
      workflow(name).fetch('jobs').each_value do |job|
        job.fetch('steps', []).each do |step|
          script = step['run'].to_s
          next unless script.include?('rsync') && script.include?('/root/.m2')
          count += 1
          refute_includes script, '--delete'
          assert_includes script, '--ignore-existing'
          assert_includes script, '--partial-dir=.rsync-partial'
          refute_match(/--partial(?:\s|$)/, script)
          assert_includes script, "--exclude='/com/tapdata/***'"
          assert_includes script, "--exclude='/io/tapdata/***'"
          assert_includes script, 'tapdata/repository/ /root/.m2/repository/'
        end
      end
    end
    assert_equal 3, count
  end

  def test_scan_uses_same_profile_and_stops_after_build_failure
    job = workflow('mr-ci.yaml').fetch('jobs').fetch('Scan-Tapdata')
    script = job.fetch('steps').find { |step| step['name'] == 'Build Tapdata - And Analyze' }.fetch('run')
    script = script.gsub(/\$\{\{.*?\}\}/m, 'fixture')
    [0, 17].each do |build_status|
      out, err, status = Open3.capture3({'SONAR_EVENT_NAME' => 'push', 'SONAR_BRANCH' => 'develop'}, 'bash', '-c', <<~SH)
        set -eo pipefail
        update-alternatives() { :; }
        cd() { :; }
        mvn() {
          printf '%s\n' "$*"
          if [[ "$1" == -U && "$2" == clean ]]; then return #{build_status}; fi
        }
        #{script}
      SH
      assert_includes out, '-U clean install -T1C -Dmaven.compile.fork=true -Dtm.skipITs=false -P idaas'
      scanner = '-U -P idaas org.sonarsource.scanner.maven:sonar-maven-plugin:sonar'
      if build_status.zero?
        assert status.success?, err
        assert_includes out, scanner
      else
        assert_equal 17, status.exitstatus
        refute_includes out, scanner
      end
    end
    assert_equal 120, job.fetch('timeout-minutes')
  end

  def test_api_bootstrap_requires_pinned_source_and_runs_api_tests_before_install
    job = workflow('mr-ci.yaml').fetch('jobs').fetch('Scan-Tapdata')
    revision = job.fetch('env').fetch('API_SOURCE_REVISION')
    assert_match(/\A[0-9a-f]{40}\z/, revision)
    checkout = job.fetch('steps').find { |step| step['name'] == 'Checkout verified API source' }.fetch('with')
    assert_equal 'tapdata/tapdata-common-lib', checkout.fetch('repository')
    assert_equal '${{ env.API_SOURCE_REVISION }}', checkout.fetch('ref')
    assert_equal false, checkout.fetch('persist-credentials')
    script = job.fetch('steps').find { |step| step['name'] == 'Build verified API for this runner' }.fetch('run')
    refute_includes script, 'skipTests'
    refute_includes script, 'deploy'
    [[revision, 1, 0], ['0' * 40, 1, 0], [revision, 0, 0], [revision, 1, 1]].each do |actual_revision, test_count, skipped|
      Dir.mktmpdir('api-bootstrap-test-') do |directory|
        project = File.join(directory, 'tapdata')
        source = File.join(directory, 'tapdata-common-lib/plugin-kit/tapdata-api')
        FileUtils.mkdir_p(File.join(project, 'iengine'))
        FileUtils.mkdir_p(source)
        namespace = 'http://maven.apache.org/POM/4.0.0'
        File.write(File.join(project, 'iengine/pom.xml'), "<project xmlns='#{namespace}'><properties><tapdata-api.version>2.0.11-fixture-SNAPSHOT</tapdata-api.version></properties></project>")
        pom = File.join(source, 'pom.xml')
        original = "<project xmlns='#{namespace}'><artifactId>tapdata-api</artifactId><version>2.0.11-SNAPSHOT</version></project>"
        File.write(pom, original)
        out, err, status = Open3.capture3({'API_SOURCE_REVISION' => revision}, 'bash', '-c', <<~SH, chdir: project)
          update-alternatives() { :; }
          git() { printf '%s\\n' '#{actual_revision}'; }
          mvn() {
            printf 'MVN:%s\\n' "$*"
            mkdir -p ../tapdata-common-lib/plugin-kit/tapdata-api/target/surefire-reports
            printf '<testsuite tests="#{test_count}" skipped="#{skipped}"/>' > ../tapdata-common-lib/plugin-kit/tapdata-api/target/surefire-reports/TEST-fixture.xml
          }
          #{script}
        SH
        if actual_revision == revision && test_count > skipped
          assert status.success?, err
          assert_includes File.read(pom), '2.0.11-fixture-SNAPSHOT'
          assert_includes File.read(pom), '<artifactId>tapdata-api</artifactId>'
          assert_includes File.read(pom), '<artifactId>maven-surefire-plugin</artifactId>'
          assert_includes File.read(pom), '<version>3.2.5</version>'
          assert_includes out, 'MVN:-B -ntp -f ../tapdata-common-lib/plugin-kit/tapdata-api/pom.xml install'
          assert_includes out, 'Verified API tests: 1'
        elsif test_count <= skipped
          refute status.success?
          assert_includes err, 'executed zero tests'
        else
          refute status.success?
          refute_includes out, 'MVN:'
          assert_equal original, File.read(pom)
        end
      end
    end
  end

  def sonar_parameters(env)
    script = workflow('mr-ci.yaml').fetch('jobs').fetch('Scan-Tapdata').fetch('steps')
      .find { |step| step['name'] == 'Build Tapdata - And Analyze' }.fetch('run')
      .split('# BEGIN sonar mode', 2).last.split('# END sonar mode', 2).first
    Open3.capture3({'SONAR_EVENT_NAME' => '', 'SONAR_BRANCH' => '',
                   'SONAR_PR_KEY' => '', 'SONAR_PR_BRANCH' => '', 'SONAR_PR_BASE' => ''}.merge(env),
                  'bash', '-c', script + "\nprintf '%s\\n' \"\${sonar_args[@]}\"\n")
  end

  def test_pr_analysis_excludes_branch_mode_and_enables_java_optimization
    out, err, status = sonar_parameters('SONAR_EVENT_NAME' => 'pull_request',
                                       'SONAR_PR_KEY' => '3515',
                                       'SONAR_PR_BRANCH' => 'codex/fix-tap-number-unit-tests',
                                       'SONAR_PR_BASE' => 'develop')
    assert status.success?, err
    assert_equal ['-Dsonar.pullrequest.key=3515',
                  '-Dsonar.pullrequest.branch=codex/fix-tap-number-unit-tests',
                  '-Dsonar.pullrequest.base=develop',
                  '-Dsonar.java.skipUnchanged=true'], out.lines.map(&:strip)
    refute_includes out, '-Dsonar.branch.name'
  end

  def test_non_pr_analysis_keeps_full_branch_analysis
    %w[push workflow_dispatch schedule].each do |event|
      out, err, status = sonar_parameters('SONAR_EVENT_NAME' => event, 'SONAR_BRANCH' => 'develop')
      assert status.success?, err
      assert_equal "-Dsonar.branch.name=develop\n", out
    end
  end

  def test_bad_or_missing_pr_metadata_is_not_silently_treated_as_branch
    [{}, {'SONAR_PR_KEY' => 'invalid'},
     {'SONAR_PR_KEY' => '3515', 'SONAR_PR_BRANCH' => 'feature', 'SONAR_PR_BASE' => ''}].each do |env|
      out, _err, status = sonar_parameters({'SONAR_EVENT_NAME' => 'pull_request'}.merge(env))
      refute status.success?
      assert_empty out
    end
  end

  def test_pr_source_branch_is_passed_literally
    branch = 'feature/$(id)'
    out, err, status = sonar_parameters('SONAR_EVENT_NAME' => 'pull_request',
                                       'SONAR_PR_KEY' => '3515', 'SONAR_PR_BRANCH' => branch,
                                       'SONAR_PR_BASE' => 'develop')
    assert status.success?, err
    assert_includes out.lines.map(&:strip), "-Dsonar.pullrequest.branch=#{branch}"
  end

  def test_checkout_validates_source_and_fetches_target_without_merging
    steps = workflow('mr-ci.yaml').fetch('jobs').fetch('Scan-Tapdata').fetch('steps')
    script = steps.find { |step| step['name'] == 'Checkout Tapdata Code' }.fetch('run')
    assert_includes script, 'SONAR_PR_HEAD_SHA'
    assert_includes script, 'refs/remotes/origin/$SONAR_PR_BASE'
    assert_includes script, 'SONAR_PR_BASE_SHA'
    refute_match(/git (merge |checkout .*merge)/, script)
    comment = steps.find { |step| step['name'] == 'Send SonarQube Quality Gate to Pr Comment' }.fetch('run')
    assert_includes comment, '--pull-request="$SONAR_PR_KEY"'
  end

  def test_regression_suite_is_automatically_run_without_private_runner_or_secrets
    regression = workflow('ci-workflow-regression.yml')
    triggers = regression['on'] || regression[true] # Psych/YAML 1.1 treats "on" as true.
    assert triggers.key?('pull_request')
    assert_equal triggers.fetch('push').fetch('paths'), triggers.fetch('pull_request').fetch('paths')
    job = regression.fetch('jobs').fetch('regression')
    assert_equal 'ubuntu-latest', job.fetch('runs-on')
    assert_equal 5, job.fetch('timeout-minutes')
    assert_equal({'contents' => 'read'}, regression.fetch('permissions'))
    checkout = job.fetch('steps').find { |step| step['uses'] == 'actions/checkout@v4' }
    assert_equal false, checkout.fetch('with').fetch('persist-credentials')
    commands = job.fetch('steps').map { |step| step['run'].to_s }.join("\n")
    assert_includes commands, 'ruby tests/ci/maven_workflow_test.rb'
    refute_includes commands, 'secrets.'
  end

  def test_all_rsync_credentials_come_from_secret_and_are_not_interpolated_into_shell
    count = 0
    %w[mr-ci.yaml build.yml].each do |name|
      workflow(name).fetch('jobs').each_value do |job|
        Array(job['steps']).each do |step|
          script = step['run'].to_s
          next unless script.include?('/tmp/rsync.passwd')
          count += 1
          label = "#{name}: #{step['name']}"
          assert_equal '${{ secrets.RSYNC_PASSWORD }}', step.fetch('env').fetch('RSYNC_PASSWORD'), label
          assert_includes script, ': "${RSYNC_PASSWORD:?RSYNC_PASSWORD secret is required}"', label
          assert_includes script, 'umask 077', label
          write = %q{printf '%s\n' "$RSYNC_PASSWORD" > /tmp/rsync.passwd}
          cleanup = %q{trap 'rm -f /tmp/rsync.passwd' EXIT}
          assert_includes script, write, label
          assert_includes script, cleanup, label
          assert_operator script.index(cleanup), :<, script.index(write), label
          assert_operator script.index('umask 077'), :<, script.index(write), label
          refute_match(/echo[^\n]*>\s*\/tmp\/rsync\.passwd/, script, label)
          refute_includes script, '${{ secrets.RSYNC_PASSWORD }}', label
        end
      end
    end
    assert_operator count, :>, 0
  end
end
