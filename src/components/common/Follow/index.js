import React, { useState, useEffect } from "react";

import axios from "axios";

import { PlusIcon, CheckMarkIcon } from "../../common/Icon";
import Button from "../Button";
import { apiURL } from "../../../appRedux/actions/helpers";

const Follow = (props) => {
  const { followId, FollowText, FollowingText } = props;

  const [watching, setWatching] = useState(props.watching);

  useEffect(() => {
    setWatching(props.watching);
  }, [props.watching]);

  const follow = async () => {
    try {
      setWatching(true);

      let url = `${apiURL}/billionaires/${followId}/follow`;
      let response = await axios.get(url);

      if (response.status === 200) {
        props.followInvestor && props.followInvestor(followId);
      }
    } catch (error) {}
  };

  const unfollow = async () => {
    try {
      setWatching(false);

      let url = `${apiURL}/billionaires/${props.followId}/unfollow`;
      let response = await axios.get(url);

      if (response.status === 200) {
        props.unfollowInvestor && props.unfollowInvestor(followId);
      }
    } catch (error) {}
  };

  return watching ? (
    <Button
      variant="primary"
      label={FollowingText}
      labelClassname="text-bold text-uppercase"
      className="follow-btn follow-active"
      icon={<CheckMarkIcon />}
      iconPosition="left"
      onClick={unfollow}
    />
  ) : (
    <Button
      variant="primary"
      label={FollowText}
      labelClassname="text-bold text-uppercase"
      className="follow-btn"
      icon={<PlusIcon />}
      iconPosition="left"
      onClick={follow}
    />
  );
};

export default Follow;

Follow.defaultProps = {
  FollowingText: "Following",
  FollowText: "Follow"
};
