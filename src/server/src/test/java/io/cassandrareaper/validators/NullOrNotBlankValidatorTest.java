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

import java.util.Set;

import javax.validation.ConstraintViolation;
import javax.validation.Validation;
import javax.validation.Validator;
import javax.validation.ValidatorFactory;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class NullOrNotBlankValidatorTest {
  @Test
  public void testNullFieldValidation() {
    ValidatorFactory factory = Validation.buildDefaultValidatorFactory();
    Validator validator = factory.getValidator();

    NullOrNotBlankTest nullOrNotBlankTest = new NullOrNotBlankTest(null);

    Set<ConstraintViolation<NullOrNotBlankTest>> violations =
        validator.validate(nullOrNotBlankTest);
    assertNotNull(violations);
    assertTrue(violations.isEmpty());
  }

  @Test
  public void testEmptyFieldValidation() {
    ValidatorFactory factory = Validation.buildDefaultValidatorFactory();
    Validator validator = factory.getValidator();

    NullOrNotBlankTest nullOrNotBlankTest = new NullOrNotBlankTest("");

    Set<ConstraintViolation<NullOrNotBlankTest>> violations =
        validator.validate(nullOrNotBlankTest);
    assertNotNull(violations);
    assertFalse(violations.isEmpty());
  }

  private static class NullOrNotBlankTest {
    @NullOrNotBlank private String test;

    NullOrNotBlankTest(String test) {
      this.test = test;
    }
  }
}
