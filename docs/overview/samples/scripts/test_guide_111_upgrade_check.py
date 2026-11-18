# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
"""Check the NVCF 1.1.1 upgrade guides: pinned versions, required sections and shared logic."""

import re
import unittest

from test_recovery_upgrade_check import DOCS, block_containing

NOTES = DOCS / "overview/release-notes"
GUIDES = {
    "1.0.4": NOTES / "1.0.4-to-1.1.1-upgrade.md",
    "1.1.0": NOTES / "1.1.0-to-1.1.1-upgrade.md",
}


def section(text, heading):
    body = text.split(heading, 1)[1]
    return re.split(r"\n## ", body, maxsplit=1)[0]


class UpgradeGuide111Checks(unittest.TestCase):
    def text(self, baseline):
        return GUIDES[baseline].read_text()

    def test_each_guide_selects_the_111_stacks(self):
        for baseline in GUIDES:
            with self.subTest(baseline=baseline):
                text = self.text(baseline)
                self.assertRegex(text, r"\|[^\n]*Self-Managed[^\n]*\|[^\n]*\b1\.1\.9\b")
                self.assertRegex(text, r"\|[^\n]*Compute Plane[^\n]*\|[^\n]*\b1\.1\.1\b")
                self.assertRegex(text, r"\|[^\n]*Observability[^\n]*\|[^\n]*\b1\.1\.0\b")

    def test_the_104_guide_names_the_target_tags_and_its_baseline(self):
        text = self.text("1.0.4")
        for tag in ("deploy/stacks/self-managed/v1.1.9",
                    "deploy/stacks/nvcf-compute-plane/v1.1.1",
                    "deploy/stacks/observability/v1.1.0"):
            self.assertIn(tag, text)
        self.assertIn("| 1.0.4 | 1.0.10 | 1.0.5 | 1.0.0 |", text)

    def test_receipt_expects_the_selected_self_managed_version(self):
        block = block_containing(GUIDES["1.1.0"], "EXPECTED_CONTROL_STACK_VERSION")
        self.assertIn('EXPECTED_CONTROL_STACK_VERSION="1.1.9"', block)
        text = self.text("1.0.4")
        self.assertIn('export STACK_VERSION="1.1.9"', text)
        self.assertIn('export COMPUTE_STACK_VERSION="1.1.1"', text)
        receipt = block_containing(GUIDES["1.0.4"], "installed_stack_version")
        self.assertIn('= "$STACK_VERSION"', receipt)

    def test_cert_manager_images_come_from_upstream(self):
        for baseline in GUIDES:
            with self.subTest(baseline=baseline):
                text = self.text(baseline)
                heading = "## cert-manager images come from upstream"
                self.assertIn(heading, text)
                body = section(text, heading)
                for component in ("controller", "webhook", "cainjector", "acmesolver", "startupapicheck"):
                    self.assertIn(component, body)
                self.assertIn("image.repository", body)

    def test_openbao_transition_matches_the_baseline(self):
        from_110 = self.text("1.1.0")
        from_104 = self.text("1.0.4")
        # 1.1.0 has no lifecycle mode set: stage bridge, rotate, versioned, once each.
        self.assertEqual(1, from_110.count('.openbao.jwtPluginLifecycle.mode = "bridge"'))
        self.assertEqual(1, from_110.count('.openbao.jwtPluginLifecycle.mode = "versioned"'))
        # 1.0.4 already ran the plugin update: no bridge step, no plugin migration.
        self.assertNotIn('.openbao.jwtPluginLifecycle.mode = "bridge"', from_104)
        self.assertNotIn('.openbao.jwtPluginLifecycle.mode = "versioned"', from_104)

    def test_the_104_guide_covers_both_ha_modes_and_prepares_enforced(self):
        text = self.text("1.0.4")
        self.assertIn("`preferred` (default)", text)
        self.assertIn("`enforced`", text)
        self.assertIn("#### Prepare for enforced mode", text)
        prep = text.split("#### Prepare for enforced mode", 1)[1].split("####", 1)[0]
        for requirement in ("Three nodes", "Three zones", "WaitForFirstConsumer", "topology.kubernetes.io/zone"):
            self.assertIn(requirement, prep)
        self.assertIn("#prepare-the-ha-mode-and-configuration", text)
        # The copy-paste sample must carry the documented default, not the stricter mode.
        sample = re.search(r"```yaml\nhighAvailability:\n  mode: (\w+)", text)
        self.assertIsNotNone(sample)
        self.assertEqual("preferred", sample.group(1))
        # The 1.1.0 baseline already has an HA mode; its guide must not re-decide it.
        self.assertNotIn("highAvailability", self.text("1.1.0"))

    def test_both_guides_explain_that_1_0_4_is_the_upgrade_stop(self):
        for baseline in GUIDES:
            with self.subTest(baseline=baseline):
                text = self.text(baseline)
                self.assertIn("## Choose your upgrade path", text)
                body = section(text, "## Choose your upgrade path")
                self.assertIn("1.0.4-to-1.1.1-upgrade.md", body)
                self.assertIn("1.1.0-to-1.1.1-upgrade.md", body)
                self.assertIn("1.0.2-to-1.0.4-upgrade.md", body)

    def test_the_110_guide_reviews_a_diff_before_each_stage_apply(self):
        text = self.text("1.1.0")
        for stage in ("01-dependencies", "02-core", "03-observability"):
            with self.subTest(stage=stage):
                diff = text.find(f"helmfile.d/{stage}.yaml.gotmpl diff --suppress-secrets")
                apply = max(text.find(f"helmfile.d/{stage}.yaml.gotmpl apply"), text.find(f"{stage}.yaml.gotmpl apply"))
                self.assertGreaterEqual(diff, 0)
                self.assertLess(diff, apply)

    def helmfile_commands(self, baseline):
        """Yield (verb, stage file, selector) for each helmfile command, in guide order."""
        text = re.sub(r"\\\n\s*", " ", self.text(baseline))
        for line in text.splitlines():
            if "helmfile.d/" not in line or "--allow-no-matching-release" in line and "-f " not in line:
                continue
            stage = re.search(r"helmfile\.d/(\S+?)\.yaml\.gotmpl", line)
            verb = re.search(r"\b(diff|apply)\b", line)
            if stage and verb and ("control_helmfile" in line):
                selector = re.search(r"--selector (\S+)", line)
                yield verb.group(1), stage.group(1), selector.group(1) if selector else ""

    def test_every_stage_apply_is_preceded_by_a_diff_of_the_same_release_set(self):
        for baseline in GUIDES:
            with self.subTest(baseline=baseline):
                seen = set()
                applies = 0
                for verb, stage, selector in self.helmfile_commands(baseline):
                    if verb == "diff":
                        seen.add((stage, selector))
                    elif stage != "04-upgrade-receipt":  # the receipt stage only records a ConfigMap
                        applies += 1
                        self.assertIn((stage, selector), seen, f"{stage} {selector} apply has no earlier diff")
                self.assertGreaterEqual(applies, 3)

    def test_no_command_block_runs_a_diff_and_the_apply_together(self):
        for baseline in ("1.1.0",):
            with self.subTest(baseline=baseline):
                blocks = re.findall(r"```bash\n(.*?)```", self.text(baseline), re.DOTALL)
                for block in blocks:
                    if "helmfile.d/" in block and " apply" in block:
                        self.assertNotIn(" diff ", block)

    def test_both_guides_carry_the_qualification_warning(self):
        for baseline in GUIDES:
            with self.subTest(baseline=baseline):
                self.assertIn("dsx-ai-factory/nvcf/issues/3428", self.text(baseline))

    def test_both_guides_upgrade_the_control_plane_before_the_compute_plane(self):
        for baseline in GUIDES:
            with self.subTest(baseline=baseline):
                text = self.text(baseline)
                control = text.index("## 4. Apply control-plane stages in order") if baseline == "1.0.4" \
                    else text.index("## 3. Apply control-plane stages in order")
                compute = re.search(r"^## \d+\. Upgrade each compute cluster", text, re.M)
                self.assertIsNotNone(compute)
                self.assertLess(control, compute.start())
                self.assertNotIn("Upgrade the existing compute plane", text)

    def test_step_one_block_sets_strict_shell_options_when_the_text_requires_them(self):
        for baseline in GUIDES:
            with self.subTest(baseline=baseline):
                text = self.text(baseline)
                step = section(text, "## 1. Set deployment values")
                if "set -euo pipefail" in step.split("```", 1)[0]:
                    block = re.search(r"```bash\n(.*?)```", step, re.DOTALL).group(1)
                    self.assertIn("set -euo pipefail", block)

    def test_nats_drain_checks_every_memory_stream_including_replicated_ones(self):
        for baseline, path in GUIDES.items():
            with self.subTest(baseline=baseline):
                match = re.search(r"jq -se '(.*?)' \\\n", path.read_text(), re.DOTALL)
                self.assertIsNotNone(match)
                # HA modes replicate every memory stream, so a single-replica filter selects nothing.
                self.assertNotIn("== 1) |", match.group(1))
                self.assertIn("No memory streams found", match.group(1))

    def test_a_stalled_rollout_has_a_generic_troubleshooting_fix(self):
        for baseline in GUIDES:
            with self.subTest(baseline=baseline):
                text = self.text(baseline)
                self.assertIn("### A release stays failed after a stalled rollout", text)
                self.assertIn("helmfile.d/02-core.yaml.gotmpl --selector name=<release> sync", text)
                self.assertNotIn("4 CPU", text)

    def test_the_bridge_step_links_a_documented_jwt_canary(self):
        runbook = (DOCS / "self-managed/runbooks/openbao-jwt-plugin-upgrade.md").read_text()
        self.assertIn("### JWT canary", runbook)
        self.assertIn("jwt_canary()", runbook)
        self.assertIn("openbao-jwt-plugin-upgrade.md#jwt-canary", self.text("1.1.0"))

    def test_the_104_guide_has_no_hostname_policy_step(self):
        text = self.text("1.0.4")
        self.assertNotIn("apply-hostname-policy", text)
        self.assertNotIn("Refresh the hostname policy", text)

    def test_nats_drain_decision_is_the_same_in_both_guides(self):
        filters = {}
        for baseline, path in GUIDES.items():
            match = re.search(r"jq -se '(.*?)' \\\n", path.read_text(), re.DOTALL)
            self.assertIsNotNone(match, baseline)
            filters[baseline] = match.group(1)
        self.assertEqual(filters["1.0.4"], filters["1.1.0"])

    def test_guides_list_the_new_settings(self):
        for baseline in GUIDES:
            with self.subTest(baseline=baseline):
                text = self.text(baseline)
                self.assertIn("accountBootstrap", text)
                self.assertIn("workerCallbackProxy", text)
                self.assertIn("llmApiGateway", text)


if __name__ == "__main__":
    unittest.main()
