/*
 * Copyright (c) 2014. Needham Software LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.solrsystem;
/*
 * Created with IntelliJ IDEA.
 * User: gus
 * Date: 3/12/14
 */

import com.copyright.easiertest.Mock;
import com.copyright.easiertest.ObjectUnderTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

import static com.copyright.easiertest.EasierMocks.*;
import static org.easymock.EasyMock.expect;

public class MainTest {
  @ObjectUnderTest private Main main;
  @Mock private File mainDirFileMock;
  @Mock private File toolsDirFileMock;

  public MainTest() {
    prepareMocks(this);
  }

  @Before
  public void setUp() {
    reset();
  }

  @After
  public void tearDown() {
    verify();
  }

  @Test
  public void testInstall() {
    main.selectUI();

    expect(main.userSaysOkToInstall()).andReturn(true);
    expect(main.createSolrSystemHome()).andReturn(mainDirFileMock);
    main.downloadGradle(mainDirFileMock);
    expect(main.createToolsDir(mainDirFileMock)).andReturn(toolsDirFileMock);
    main.unpackGradleInto(toolsDirFileMock);

    replay();
    main.install();
  }

  @Test
  public void testInstallDeclined() {
    main.selectUI();

    expect(main.userSaysOkToInstall()).andReturn(false);
    replay();
    main.install();
  }

}
