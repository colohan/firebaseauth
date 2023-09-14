/* eslint-disable space-before-function-paren */
/**
 * Copyright 2023 Chris Colohan.  All Rights Reserved.
 *
 * Based on Firebase sample code from Google that bears this copyright:
 *
 * Copyright 2015 Google Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
'use strict';

import { initializeApp } from 'firebase/app';

import {
  getAuth,
  signOut,
  connectAuthEmulator,
  onAuthStateChanged,
  GoogleAuthProvider,
  signInWithRedirect
} from 'firebase/auth';

const spinnerElement = document.getElementById('spinner');

// ---------------------------------------------------------------------------
// Display a spinner when we are loading a page:
function spinnerOn() {
  spinnerElement.style.display = 'flex';
  spinnerElement.style.opacity = 1;
}

function spinnerOff() {
  function delay(time) {
    return new Promise(resolve => setTimeout(resolve, time));
  }

  spinnerElement.style.opacity = 0;
  delay(1000).then(() => {
    spinnerElement.style.display = 'none';
  });
}

// ---------------------------------------------------------------------------
// Remove all event listeners from a DOM node.
export function removeEventListeners(node) {
  const newNode = node.cloneNode(true);
  node.parentNode.replaceChild(newNode, node);
}

// ---------------------------------------------------------------------------
// Hide/show the specified HTML element:
export function hideElement(element) {
  element.setAttribute('hidden', '');
}

export function showElement(element) {
  element.removeAttribute('hidden');
}

// ---------------------------------------------------------------------------
// Display a dialog wth a message and a button.  Note that we use innerHTML for
// the title and contents, so any user-input in there needs to be escaped.
const dialogElement = document.getElementById('error_dialog');

export function showDialog(title, contents, button1, action1, button2, action2) {
  dialogElement.querySelector('h4').innerHTML = title;
  dialogElement.querySelector('div > p').innerHTML = contents;

  removeEventListeners(dialogElement.querySelector('#error1'));
  removeEventListeners(dialogElement.querySelector('#error2'));
  const error1 = dialogElement.querySelector('#error1');
  const error2 = dialogElement.querySelector('#error2');
  error1.innerHTML = button1;
  error1.addEventListener('click', action1);
  if (button2) {
    error2.innerHTML = button2;
    error2.addEventListener('click', action2);
    showElement(error2);
  } else {
    hideElement(error2);
  }
  dialogElement.showModal();
}
export function closeDialog() {
  dialogElement.close();
}

// ---------------------------------------------------------------------------
// Set up Firebase
initializeApp({
  apiKey: 'xxx',
  authDomain: 'www.xxx.com'
});
connectAuthEmulator(getAuth(), 'http://localhost:9099');

// ---------------------------------------------------------------------------
// One-time initialization, called when the webpage first loads.
function initApp() {
  spinnerOn();
  // Until we are signed in, don't do anything else:
  onAuthStateChanged(getAuth(), function (user) {
    if (user) {
      spinnerOff();
      console.log('Signed in');
      showDialog('Signed In!', 'You are now signed in', 'Sign Out', () => { signOut(getAuth()); });
    } else {
      spinnerOff();
      console.log('Signed out');

      async function signIn() {
        const provider = new GoogleAuthProvider();
        await signInWithRedirect(getAuth(), provider);
      }

      showDialog('Welcome!', 'This is working on Edge, but not Chrome.  Why?',
        '<i class="material-icons">account_circle</i>Click to Sign-in', signIn);
    }
  });
}

window.onload = () => {
  initApp();
};
