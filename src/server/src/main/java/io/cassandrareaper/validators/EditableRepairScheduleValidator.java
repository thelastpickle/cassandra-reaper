/*
 * Copyright 2021-2021 DataStax, Inc.
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

package io.cassandrareaper.validators;

import io.cassandrareaper.core.EditableRepairSchedule;

import javax.validation.ConstraintValidator;
import javax.validation.ConstraintValidatorContext;

public class EditableRepairScheduleValidator
    implements ConstraintValidator<ValidEditableRepairSchedule, EditableRepairSchedule> {
  @Override
  public void initialize(ValidEditableRepairSchedule validEditableRepairSchedule) {
  }

  @Override
  public boolean isValid(EditableRepairSchedule editableRepairSchedule, ConstraintValidatorContext context) {
    return editableRepairSchedule.getOwner() != null
        || editableRepairSchedule.getRepairParallelism() != null
        || editableRepairSchedule.getIntensity() != null
        || editableRepairSchedule.getDaysBetween() != null
        || editableRepairSchedule.getSegmentCountPerNode() != null;
  }
}
