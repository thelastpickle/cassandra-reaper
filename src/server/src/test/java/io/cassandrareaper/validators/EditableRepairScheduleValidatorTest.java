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

import java.util.Set;

import javax.validation.ConstraintViolation;
import javax.validation.Validation;
import javax.validation.Validator;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.apache.cassandra.repair.RepairParallelism;
import org.junit.Test;

public class EditableRepairScheduleValidatorTest {
  private static Validator validator = Validation.buildDefaultValidatorFactory().getValidator();

  @Test
  public void testEditableRepairScheduleWithAllNulls() {
    EditableRepairSchedule editableRepairSchedule = new EditableRepairSchedule();

    Set<ConstraintViolation<EditableRepairSchedule>> violations =
        validator.validate(editableRepairSchedule);
    assertNotNull(violations);
    // We should expect a violation because none of the fields are present
    assertFalse(violations.isEmpty());
  }

  @Test
  public void testEditableRepairScheduleWithOneValid() {
    EditableRepairSchedule editableRepairSchedule = new EditableRepairSchedule();
    editableRepairSchedule.setRepairParallelism(RepairParallelism.SEQUENTIAL);

    Set<ConstraintViolation<EditableRepairSchedule>> violations =
        validator.validate(editableRepairSchedule);
    assertNotNull(violations);
    // We should not expect a violation because at least one of the fields was valid
    assertTrue(violations.isEmpty());
  }

  @Test
  public void testEditableRepairScheduleWithInvalidOwner() {
    EditableRepairSchedule editableRepairSchedule = new EditableRepairSchedule();
    editableRepairSchedule.setOwner("");

    Set<ConstraintViolation<EditableRepairSchedule>> violations =
        validator.validate(editableRepairSchedule);
    assertNotNull(violations);
    // We should expect a violation
    assertFalse(violations.isEmpty());
    // We should expect a violation of the "owner" field because when it's provided, it can't be
    // empty
    // This is indirectly testing the NullOrNotBlankValidator as well as the higher-level logic
    ConstraintViolation ownerViolation =
        violations.stream()
            .filter(
                violation ->
                    violation.getPropertyPath() != null
                        && violation.getPropertyPath().toString().equals("owner"))
            .findAny()
            .orElse(null);
    assertNotNull(ownerViolation);
  }
}
