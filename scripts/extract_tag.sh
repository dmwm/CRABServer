#!/bin/bash

if [[ "${CI_COMMIT_TAG}" =~ ^pypi-.*-([0-9]+)$ ]]; then
  TAG_NUMBER="${BASH_REMATCH[1]}"
  IMAGE_TAG="${CI_COMMIT_TAG}"
  echo "Extracted tag number: ${TAG_NUMBER}"
  NEW_TAG_NUMBER=$((${TAG_NUMBER} - 1))
  BASE_TAG="pypi-$(echo ${CI_COMMIT_TAG} | sed -E 's/[0-9]+$/'"${NEW_TAG_NUMBER}"'/)"
  echo "New tag: ${BASE_TAG}"
  echo "${IMAGE_TAG}" > image_tag.txt
  echo "${BASE_TAG}" > base_tag.txt
else
  echo "The CI_COMMIT_TAG does not match the expected pattern."
  exit 1
fi