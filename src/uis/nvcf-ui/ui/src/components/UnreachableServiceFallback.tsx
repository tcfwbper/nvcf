/**
 * SPDX-FileCopyrightText: Copyright (c) NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { Button, StatusMessage } from "@nvidia/foundations-react-core";
import { ExternalLinkIcon, OctagonAlertIcon } from "lucide-react";

// Also hardcoded in nvcf-cli: internal/selfhosted/preflight.go and
// internal/selfhosted/progress/render_tty.go.
export const TROUBLESHOOTING_DOCS_URL =
	"https://docs.nvidia.com/nvcf/self-hosted/troubleshooting";

interface UnreachableServiceFallbackProps {
	onRetry: () => void;
}

export function UnreachableServiceFallback({
	onRetry,
}: UnreachableServiceFallbackProps) {
	return (
		<div className="grid h-full place-items-center p-8">
			<StatusMessage
				size="medium"
				slotFooter={
					<div className="flex gap-2">
						<Button kind="secondary" onClick={onRetry}>
							Try again
						</Button>
						<Button asChild kind="secondary">
							<a
								href={TROUBLESHOOTING_DOCS_URL}
								rel="noreferrer"
								target="_blank"
							>
								<ExternalLinkIcon size="1em" /> View Documentation
							</a>
						</Button>
					</div>
				}
				slotHeading="Unable to reach the deployed service"
				slotMedia={<OctagonAlertIcon size={32} />}
				slotSubheading="Refer to the troubleshooting documentation for common problems and how they can be resolved."
			/>
		</div>
	);
}
